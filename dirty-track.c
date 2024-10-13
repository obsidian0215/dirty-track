#include <linux/module.h>
#include <linux/device.h>
#include <linux/moduleparam.h>
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/mm.h>
#include <linux/huge_mm.h>
#include <linux/xarray.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/ioctl.h>
#include <linux/kthread.h>
#include <linux/file.h>
#include <linux/fs_struct.h>
#include <linux/dcache.h>
#include <linux/list.h>
#include <linux/mm_types.h>
#include <linux/pgtable.h>
#include <linux/swap.h>
#include <linux/swapops.h>
#include <linux/hrtimer.h>
#include <linux/ktime.h>
#include <linux/delay.h>
#include <linux/workqueue.h>
#include <linux/completion.h>
#include <linux/capability.h>
#include <linux/cdev.h>
#include <linux/timekeeping.h>
#include <linux/mmu_notifier.h>
#include <asm/tlbflush.h>
#include <linux/mm_inline.h>
#include <linux/ftrace.h>
#include <linux/kallsyms.h>
#include <linux/hugetlb.h>

#define DEVICE_NAME "dirty-track"
#define DIRTY_TRACK_MAGIC 'd'
dev_t dev;
static struct class* dirty_track_class = NULL;
static struct cdev dirty_track_cdev;

#define IOCTL_SET_DIRTY_MAP_PATH _IOW(DIRTY_TRACK_MAGIC, 1, char[256])
#define IOCTL_START_PID _IOW(DIRTY_TRACK_MAGIC, 2, pid_t)
#define IOCTL_STOP_PID _IOW(DIRTY_TRACK_MAGIC, 3, pid_t)
#define IOCTL_GET_DIRTY_MAP_PATH _IOR(DIRTY_TRACK_MAGIC, 5, char[256])

// 页表项复位的初始延时，单位为ns --> 3ms
#define INIT_DELAY 3000000
// 页表项复位的最大延时，单位为ns --> 1s
#define MAX_DELAY 1000000000
// 最大可跟踪进程数
#define MAX_TRACKED_PROCESSES 24

static atomic_t tracked_processes = ATOMIC_INIT(0); // 当前跟踪的进程数

// 页面类型
#define PAGE_PTE 0                  // 4KB
#define PAGE_PMD 1                  // 2MB
#define PAGE_PUD 2                  // 1GB(当前设计下不会被使用)

static char tmpfs_dir[256];

// 单个进程的脏页追踪实例
typedef struct dirty_track {
    pid_t pid;                              // 目标进程的pid
    struct mm_struct *mm;                   // 目标进程的地址空间

    struct task_struct *track_worker;       // 脏页追踪内核线程

    char dirty_map_path[256];               // 定位保存dirty-map的共享内存(tmpfs文件)
    struct xarray dirty_xarray;             // 记录进程页写入次数的dirty-map(Xarray)    
    bool dirty_map_updated;                 // 是否更新过dirty-map

    struct list_head list;                  // 脏页追踪实例的链表节点

    bool soft_cleared;                      // 是否清除过soft-dirty位
    
    unsigned long delay_timer;     // 页表项处理延时，单位为ns
    unsigned int delay_penalty;             // 延迟惩罚因子，初始为1
    
} dirty_track_t;

// 保存单个页的修改历史
typedef struct dirty_address {
    /* unsigned long address;       // 本次写错误的地址，已被xarray索引替代 */
    unsigned long write_count;      // 写入错误次数
    unsigned int page_type;         // 页类型
} dirty_address_t;

// 脏页追踪线程的双向链表头
static LIST_HEAD(dirty_track_list);
// 脏页追踪线程的链表rw锁
static rwlock_t dirty_track_rwlock;

// 通知内核线程已经停止的相关结构
typedef struct wqtask_completion {
    struct completion wq_comp;     // 用于通知任务完成
    struct list_head list;      // 用于链表
} wqtask_completion_t;
static LIST_HEAD(wqtask_completion_list);

// 非阻塞式停止脏页追踪实例
typedef struct nbstop_kthread {
    struct work_struct work;
    dirty_track_t *dti;
    wqtask_completion_t *wq_comp;
} nbstop_kthread_t;

// 非阻塞式停止内核线程的工作队列
static struct workqueue_struct *nbstop_kthread_wq;

// 文件操作函数声明
static long device_ioctl(struct file *file, unsigned int cmd, unsigned long arg);
static int device_open(struct inode *inode, struct file *file);
static int device_release(struct inode *inode, struct file *file);
extern void flush_tlb_mm_range(struct mm_struct *mm, unsigned long start,
				unsigned long end, unsigned int stride_shift,
				bool freed_tables);

// 判断mm_struct是否完全可以被释放
bool mm_struct_can_be_freed(struct mm_struct *mm)
{
    if (mm == NULL) {
        return true;
    }
    // 检查mm_users，判断是否有进程在使用该地址空间
    if (atomic_read(&mm->mm_users)) {
        // 地址空间仍有进程在使用
        return false;
    } else if (atomic_read(&mm->mm_count) == 0) {
        // mm_count也为0，说明可以被释放
        return true;
    }
    return false;
}

// 将xarray序列化为数组，并保存在tmpfs文件中
// xarray仅内核可用，用户态内无等价实现
// 需要将xarray索引(即脏页地址)一起序列化
static inline void dirty_map_to_file(struct xarray *xarray, struct file *file, loff_t *pos) {
    unsigned long address;
    dirty_address_t *entry;

    // 遍历xarray，输出索引（页地址）和脏页统计数据
    xa_for_each(xarray, address, entry) {
        // 先写入页地址（索引）
        kernel_write(file, (char *)&address, sizeof(address), &file->f_pos);

        // 再写入脏页统计数据
        kernel_write(file, (char *)&entry->write_count, sizeof(entry->write_count), &file->f_pos);
        kernel_write(file, (char *)&entry->page_type, sizeof(entry->page_type), &file->f_pos);
    }
}

// 将xarray中指定的索引项删除
static inline void xarray_remove(struct xarray *xarray, unsigned long address) {
    dirty_address_t *entry;

    entry = xa_erase(xarray, address);
    if (entry) {
        kfree(entry);
        // printk(KERN_DEBUG "xarray_remove: 已删除地址 0x%lx 从xarray\n", address);
    } else {  
        // printk(KERN_DEBUG "xarray_remove: 地址 0x%lx 未在xarray中找到，跳过删除\n", address);
        return;
    }
}

// 将xarray中指定的索引项的write_count减1
// 若write_count减为0，则删除该索引项
static inline void xarray_dec_write_count(struct xarray *xarray, unsigned long address) {
    dirty_address_t *entry;

    entry = xa_load(xarray, address);
    if (entry) {
        entry->write_count--;
        if (entry->write_count <= 0)
            xarray_remove(xarray, address);
    }
}

// 如果vma->vm_flags包含VM_WRITE，则将pte设置为可写
static inline pte_t mb_mkwrite(pte_t pte, struct vm_area_struct *vma)
{
	if (likely(vma->vm_flags & VM_WRITE))
		pte = pte_mkwrite(pte);
	return pte;
}

// 如果vma->vm_flags包含VM_WRITE，则将pmd设置为可写
static inline pmd_t mb_pmd_mkwrite(pmd_t pmd, struct vm_area_struct *vma)
{
	if (likely(vma->vm_flags & VM_WRITE))
		pmd = pmd_mkwrite(pmd);
	return pmd;
}

static inline bool pte_is_pinned(struct vm_area_struct *vma, unsigned long addr, pte_t pte)
{
	struct folio *folio;

	if (!pte_write(pte))
		return false;
	if (!is_cow_mapping(vma->vm_flags))
		return false;
	if (likely(!test_bit(MMF_HAS_PINNED, &vma->vm_mm->flags)))
		return false;
	folio = vm_normal_folio(vma, addr, pte);
	if (!folio)
		return false;
	return folio_maybe_dma_pinned(folio);
}

/* check soft-dirty's and update dirty-map */
// 检查pmd的soft dirty标志位
// 若被设置则表明发生写入，更新脏页映射并返回true
// 若未被设置则表明未发生写入，返回false
static inline bool check_pmd_update_dirty_map(dirty_track_t *dti, pmd_t *pmdp, 
            unsigned long addr, struct vm_area_struct *vma) {
    pmd_t pmd = *pmdp;
    dirty_address_t *addr_dirty;

    if ((pmd_present(pmd) && pmd_soft_dirty(pmd)) || (is_swap_pmd(pmd) && pmd_swp_soft_dirty(pmd))) {
        // 标记dirty-map被更新
        if (!dti->dirty_map_updated)
            dti->dirty_map_updated = true;

        // 从pid对应的xarray中查找该地址对应页的写错误次数
        addr_dirty = xa_load(&dti->dirty_xarray, addr);
        if (addr_dirty) {
            // 此次页错误为soft-dirty，则写错误次数+1
            addr_dirty->write_count++;
            addr_dirty->page_type = PAGE_PMD;
        } else {
            // 初始化新的页记录并加入dirty-map
            addr_dirty = kzalloc(sizeof(*addr_dirty), GFP_KERNEL);
            if (!addr_dirty) {
                printk(KERN_ERR "No memory for new entry of dirty-map.\n");
                return true;
            }
            addr_dirty->write_count = 1;
            addr_dirty->page_type = PAGE_PMD;

            xa_store(&dti->dirty_xarray, addr, addr_dirty, GFP_KERNEL);
        }

        return true;
    } else {
        return false;
    }
}

// 检查pte的soft dirty标志位是否设置
// 若被设置则表明发生写入，更新脏页映射并返回true
// 若未被设置则表明未发生写入，返回false
static inline bool check_pte_update_dirty_map(dirty_track_t *dti, pte_t *ptep, 
            unsigned long addr, struct vm_area_struct *vma) {
    pte_t pte = *ptep;
    dirty_address_t *addr_dirty;

    if ((pte_present(pte) && pte_soft_dirty(pte)) || (is_swap_pte(pte) && pte_swp_soft_dirty(pte))) {
        // 标记dirty-map被更新
        if (!dti->dirty_map_updated)
            dti->dirty_map_updated = true;

        // 从pid对应的xarray中查找该地址对应页的写错误次数
        addr_dirty = xa_load(&dti->dirty_xarray, addr);
        if (addr_dirty) {
            // 此次页错误为soft-dirty，则写错误次数+1
            addr_dirty->write_count++;
            addr_dirty->page_type = PAGE_PTE;
        } else {
            // 初始化新的页记录并加入dirty-map
            addr_dirty = kzalloc(sizeof(*addr_dirty), GFP_KERNEL);
            if (!addr_dirty) {
                printk(KERN_ERR "No memory for new entry of dirty-map.\n");
                return true;
            }
            addr_dirty->write_count = 1;
            addr_dirty->page_type = PAGE_PTE;

            xa_store(&dti->dirty_xarray, addr, addr_dirty, GFP_KERNEL);
        }
        return true;
    } else {
        return false;
    }
}
 
/* clear soft-dirty's */
// 清除pmd的soft dirty标志位
static inline void clear_pmd_soft_dirty(pmd_t *pmdp, unsigned long addr, 
            struct vm_area_struct *vma) {
	pmd_t old, pmd = *pmdp;

	if (pmd_present(pmd)) {
		/* See comment in change_huge_pmd() */
		old = pmdp_invalidate(vma, addr, pmdp);
		if (pmd_dirty(old))
			pmd = pmd_mkdirty(pmd);
		if (pmd_young(old))
			pmd = pmd_mkyoung(pmd);

		pmd = pmd_wrprotect(pmd);
		pmd = pmd_clear_soft_dirty(pmd);

		set_pmd_at(vma->vm_mm, addr, pmdp, pmd);
	} else if (is_migration_entry(pmd_to_swp_entry(pmd))) {
		pmd = pmd_swp_clear_soft_dirty(pmd);
		set_pmd_at(vma->vm_mm, addr, pmdp, pmd);
	}
}

// 清除pte的soft dirty标志位
static inline void clear_pte_soft_dirty(pte_t *pte, unsigned long addr, 
            struct vm_area_struct *vma) {
	// pte_t ptent = ptep_get(pte);
    pte_t ptent = *pte;

	if (pte_present(ptent)) {
		pte_t old_pte;

		if (pte_is_pinned(vma, addr, ptent))
			return;
		old_pte = ptep_modify_prot_start(vma, addr, pte);
		ptent = pte_wrprotect(old_pte);
		ptent = pte_clear_soft_dirty(ptent);
		ptep_modify_prot_commit(vma, addr, pte, old_pte, ptent);
	} else if (is_swap_pte(ptent)) {
		ptent = pte_swp_clear_soft_dirty(ptent);
		set_pte_at(vma->vm_mm, addr, pte, ptent);
	}
}

// 读取并清除pmd粒度及以下所有页表项的soft dirty标志位
static int handle_pmd_range_wp(pmd_t *pmd, unsigned long addr, 
            unsigned long end, struct vm_area_struct *vma, dirty_track_t *dti) {
    // pte_t *pte, ptent;
    pte_t *pte;
	spinlock_t *ptl;
    bool need_clear = true;

    ptl = pmd_trans_huge_lock(pmd, vma);
    if (ptl) {
        // // 确保只有trans_Huge pmd进行soft-dirty
        // pmd_t pmdval = pmd_read_atomic(pmd);
        // if (pmd_bad(pmd_wrprotect(pmdval))) {
        //     printk(KERN_ALERT "Not trans_huge pmd: 0x%p, value: 0x%lx at addr: 0x%lx\n", pmd, pmdval, addr);
            
        //     // spin_unlock(ptl);
        //     goto no_clear;
        // }

        if (dti->soft_cleared)
            need_clear = check_pmd_update_dirty_map(dti, pmd, addr, vma);          

clear:
        if (need_clear)
            clear_pmd_soft_dirty(pmd, addr, vma);
no_clear:
        spin_unlock(ptl);
		return 0;
    }

    if (pmd_trans_unstable(pmd))
		return 0;

    pte = pte_offset_map_lock(vma->vm_mm, pmd, addr, &ptl);
    for (; addr != end; pte++, addr += PAGE_SIZE) {
        need_clear = true;
        // ptent = ptep_get(pte);
        if (dti->soft_cleared)
            need_clear = check_pte_update_dirty_map(dti, pte, addr, vma);

        if (need_clear)
            clear_pte_soft_dirty(pte, addr, vma);
    }
    pte_unmap_unlock(pte - 1, ptl);
    cond_resched();
    return 0;
}

// 遍历vma内部所有页表项并清除其soft dirty标志位
static int walk_clear_wp(dirty_track_t *dti, unsigned long addr, 
            unsigned long end, struct vm_area_struct *vma) {
    int err = 0;
    pgd_t *pgd;
    p4d_t *p4d;
    pud_t *pud;
    pmd_t *pmd;
	unsigned long next;

    // if (dti->soft_cleared && !(vma->vm_flags & VM_WRITE)) {
        // printk(KERN_INFO "unwriteable vma: %lx-%lx, flags: %lx", vma->vm_start, vma->vm_end, vma->vm_flags);
    //     return 0;
    // }

    pgd = pgd_offset(dti->mm, addr);
    do {
        next = pgd_addr_end(addr, end);
        if (pgd_none_or_clear_bad(pgd)) {
            continue;
        }

        if (is_hugepd(__hugepd(pgd_val(*pgd)))) {
            continue;
        }

        p4d = p4d_offset(pgd, addr);
        do {
            next = p4d_addr_end(addr, end);
            if (p4d_none_or_clear_bad(p4d)) {
                continue;
            }

            if (is_hugepd(__hugepd(pgd_val(*p4d)))) {
                continue;
            }

            pud = pud_offset(p4d, addr);
            do {
again_pud:        
                next = pud_addr_end(addr, end);
                if (pud_none(*pud)) {
                    continue;
                }
                if (vma)
                    split_huge_pud(vma, pud, addr);
                if (pud_none(*pud)) {
                    goto again_pud;
                }

                // 注意处理大页
                if (is_hugepd(__hugepd(pud_val(*pud)))) {
                    continue;
                }

                pmd = pmd_offset(pud, addr);
                do {
again_pmd:
                    next = pmd_addr_end(addr, end);
                    if (pmd_none(*pmd)) {
                        continue;
                    }

                    err = handle_pmd_range_wp(pmd, addr, next, vma, dti);
                    if (err)
                        break;
                } while (pmd++, addr = next, addr != end);
            } while (pud++, addr = next, addr != end);
        } while (p4d++, addr = next, addr != end);
    } while (pgd++, addr = next, addr != end);

    return err;
}

// // 将符合追踪条件的vma加入dti的追踪列表中s
// static int add_vma_to_dti(dirty_track_t *dti, unsigned long start, 
//             unsigned long end, struct vm_area_struct *vma) {
//     struct vma_info *vma_entry;
//     if (!vma || start >= end)
//         return 0;
    
//     vma_entry = kmalloc(sizeof(struct vma_info), GFP_KERNEL);
//     if (!vma_entry) {
//         printk(KERN_ERR "Failed to allocate memory for vma_info\n");
//         return -ENOMEM;
//     }
//     kfree(vma_entry);
//     return 0;
// }

// 遍历进程的所有vma，并调用回调函数处理
static int traverse_vmas(dirty_track_t *dti, int (*callback)(dirty_track_t *, 
            unsigned long, unsigned long, struct vm_area_struct *)) {
    unsigned long start = 0, next, end = -1;
    struct vm_area_struct *vma, *walk_vma;
    int err = 0;

    if (!dti->mm) {
        return -EINVAL;
    }

    mmap_assert_locked(dti->mm);

    vma = find_vma(dti->mm, start);
    do {
        if (!vma) {    // 遍历完所有vma
            walk_vma = NULL;
			next = end;
        }
        else if (start < vma->vm_start) {      // 处于vma外
            walk_vma = NULL;
            next = min(end, vma->vm_start);
        }
        else {      // 处于vma内
            walk_vma = vma;
            next = min(end, vma->vm_end);
            vma = find_vma(dti->mm, vma->vm_end);
            // 忽略以下vma：PFN映射、不可写、hugetlb页
            if ((walk_vma->vm_flags & VM_PFNMAP) || 
                    !(walk_vma->vm_flags & VM_WRITE) || 
                    is_vm_hugetlb_page(walk_vma))
                continue;
            if (callback)
                err = callback(dti, start, next, walk_vma);
        }
        if (err)
            break;
    } while (start = next, start < end);
    return err;
}

// 清除进程所有vma的soft-dirty标志位
static int clear_soft_dirty_once(dirty_track_t *dti) {
    struct mm_struct *mm = dti->mm;
	struct vm_area_struct *vma;
    int err = 1;

    if (mm) {
        MA_STATE(mas, &mm->mm_mt, 0, 0);
        struct mmu_notifier_range range;
        if (mmap_write_lock_killable(mm)) {
            err = -EBUSY;
			goto out;
		}

        mas_for_each(&mas, vma, ULONG_MAX) {
            if (!(vma->vm_flags & VM_SOFTDIRTY))
                continue;
            vma->vm_flags &= ~VM_SOFTDIRTY;
            vma_set_page_prot(vma);
		}

        inc_tlb_flush_pending(mm);
		mmu_notifier_range_init(&range, MMU_NOTIFY_SOFT_DIRTY,
					0, NULL, mm, 0, -1UL);
		mmu_notifier_invalidate_range_start(&range);
        err = traverse_vmas(dti, walk_clear_wp);
        mmu_notifier_invalidate_range_end(&range);
		flush_tlb_mm_range(mm, 0UL, -1UL, 0UL, true);
		dec_tlb_flush_pending(mm);
        mmap_write_unlock(mm);
out:
    }

    return err;
}

// 向指定pid进程的clear_refs写入4以启用soft-dirty tracking
static int write_clear_refs_pid(pid_t pid) {
    char *argv[] = {"/bin/bash", "-c", NULL, NULL};
    char cmd[256];
    char *envp[] = {"HOME=/", "PATH=/sbin:/bin:/usr/sbin:/usr/bin", NULL};
    int ret;

    // 设置目标PID并构建shell命令
    snprintf(cmd, sizeof(cmd), "echo 4 > /proc/%d/clear_refs", pid);

    argv[2] = cmd;

    // 执行shell命令
    ret = call_usermodehelper(argv[0], argv, envp, UMH_WAIT_PROC);
    if (ret != 0) {
        printk(KERN_ERR "Enabling soft-dirty tracking failed: %d\n", ret);
    } else {
        printk(KERN_INFO "Soft-dirty tracking of process %d enabled\n", pid);
    }
    return ret;
}

// 脏页追踪线程的主函数
static int wp_fault_track(void *data) {
    dirty_track_t *dti = (dirty_track_t *)data;
    struct vm_area_struct *vma;
    struct vma_info *vma_entry;
    pid_t pid = dti->pid;
    int ret = 0;
    // 测量时间
    ktime_t start, end;
    s64 delta_ns;
    
    start = ktime_get();
    ret = clear_soft_dirty_once(dti);
    // ret = write_clear_refs_pid(pid);
    end = ktime_get();
    delta_ns = ktime_to_ns(ktime_sub(end, start));
    if (!ret) {
        printk(KERN_INFO "[PID %d]first clear_soft_dirty_once's execution time: %lld ns\n", pid, delta_ns);
        dti->soft_cleared = true;
    }
    else {
        printk(KERN_ERR "[PID %d]first clear_soft_dirty_once has encountered an error %d\n", pid, ret);       
        return -EFAULT;
    }

    while (!kthread_should_stop()) {
        if (mm_struct_can_be_freed(dti->mm)) {
            printk(KERN_INFO "[PID %d]Trackee mm_struct can be freed, we should stop dirty-tracking\n", pid);
            break;
        }
        else {
            dti->dirty_map_updated = false;
            start = ktime_get();
            ret = clear_soft_dirty_once(dti);
            end = ktime_get();
            delta_ns = ktime_to_ns(ktime_sub(end, start));

            if (ret) {
                printk(KERN_ERR "[PID %d]clear_soft_dirty_once has encountered an error %d\n", pid, ret);
                break;
            }
            else {
                // 检查dirty_map是否有更新或xarray是否为空
                bool need_wait = false;
                if (!dti->dirty_map_updated || xa_empty(&dti->dirty_xarray)) {
                    need_wait = true;
                }

                // 满足上述条件则逐步增加执行周期
                if (need_wait) {
                    // printk(KERN_INFO "[PID %d]No write detected, waiting for %lu ms\n", pid, dti->delay_timer / NSEC_PER_MSEC);
                    dti->delay_penalty *= 2;
                    dti->delay_timer = INIT_DELAY * dti->delay_penalty;
                    // 避免delay_timer过大
                    if (dti->delay_timer > MAX_DELAY) {
                        dti->delay_timer = MAX_DELAY;
                    }
                    msleep(dti->delay_timer / NSEC_PER_MSEC);
                } else {
                    // 根据delta_ns调整delay_timer
                    dti->delay_penalty = 1;
                    if (delta_ns > dti->delay_timer) {
                        dti->delay_timer = delta_ns + INIT_DELAY;
                    } else if (dti->delay_timer > 2 * delta_ns) {
                        dti->delay_timer = INIT_DELAY;
                    } else {
                        dti->delay_timer = 2 * delta_ns;
                    }
                    msleep(dti->delay_timer / NSEC_PER_MSEC);  // 需要确定检查间隔
                }           
                // printk(KERN_INFO "[PID %d]clear_soft_dirty_once's execution time: %lld ns\n", pid, delta_ns);
            }
        }
    }

    printk(KERN_INFO "[1]Stopped dirty-tracking PID %d\n", pid);
    return ret;
}

// 工作队列nbstop_kthread_wq的处理函数
static void nbstop_kthread_fn(struct work_struct *work) {
    nbstop_kthread_t *sw = container_of(work, nbstop_kthread_t, work);
    dirty_track_t *dti = sw->dti;
    wqtask_completion_t *wqtc = sw->wq_comp;

    if (!dti) {
        printk(KERN_ERR "No dirty_track instance provided to stop\n");
        complete(&wqtc->wq_comp);
        kfree(sw);
        return;
    }

    // 停止内核线程
    if (!kthread_stop(dti->track_worker)) {
        printk(KERN_INFO "[2]Successfully stopped tracker for PID %d\n", dti->pid);
    } else {
        printk(KERN_WARNING "Failed to stop tracker for PID %d\n", dti->pid);
    }

    // 写入dirty_map文件
    if (!xa_empty(&dti->dirty_xarray)) {
        struct file *file = filp_open(dti->dirty_map_path, O_WRONLY | O_CREAT, 0644);
        if (!IS_ERR(file)) {
            loff_t pos = 0;
            dirty_map_to_file(&dti->dirty_xarray, file, &pos);
            filp_close(file, NULL);
        } else {
            printk(KERN_ERR "Failed to open dirty-map file for PID %d: %ld\n", dti->pid, PTR_ERR(file));
        }
    } else
        printk(KERN_INFO "Empty dirty-map for PID %d\n", dti->pid);


    // 清理dirty_xarray
    unsigned long addr;
    dirty_address_t *addr_dirty;
    xa_for_each(&dti->dirty_xarray, addr, addr_dirty) {
        kfree(addr_dirty);
    }
    xa_destroy(&dti->dirty_xarray);
    
    // 解除对进程mm的引用
    mmput(dti->mm);
    kfree(dti);

    complete(&wqtc->wq_comp);   // 通知内核线程已停止
    kfree(sw);
}

// 创建并启动新的脏页追踪
static int start_dirty_track(pid_t pid) {
    dirty_track_t *dti;
    struct task_struct *task;
    struct timespec64 ts;
    char timestamp[16];  // 用于存储时间戳
    int err;

    // 检查是否已跟踪此 PID
    read_lock(&dirty_track_rwlock);
    list_for_each_entry(dti, &dirty_track_list, list) {
        if (dti->pid == pid) {
            read_unlock(&dirty_track_rwlock);
            printk(KERN_ALERT "PID %d is already being tracked.\n", pid);
            return -EEXIST;
        }
    }
    read_unlock(&dirty_track_rwlock);

    // 检查是否已达到最大跟踪进程数
    if (atomic_read(&tracked_processes) >= MAX_TRACKED_PROCESSES) {
        printk(KERN_ALERT "Failed to start tracking PID %d: Max tracked processes reached\n", pid);
        return -ENOMEM;
    }

    // 创建新的脏页追踪实例
    dti = kzalloc(sizeof(*dti), GFP_KERNEL);
    if (!dti)
        return -ENOMEM;
    
    // 初始化控制字段
    dti->soft_cleared = false;
    dti->dirty_map_updated = false;
    dti->delay_timer = INIT_DELAY;
    dti->delay_penalty = 1;

    // 初始化pid以及mm_struct字段
    dti->pid = pid;
    task = pid_task(find_get_pid(pid), PIDTYPE_PID);
    if (!task) {
        printk(KERN_ERR "Cannot find task for PID: %d\n", pid);
        kfree(dti);
        return -ESRCH;
    }
    dti->mm = get_task_mm(task);
    if (!dti->mm) {
        printk(KERN_ERR "NULL mm_struct pointer (kernel thread?)\n");
        kfree(dti);
        return -EINVAL;
    }

    // 获取当前时间
    ktime_get_real_ts64(&ts);

    // 获取毫秒级时间戳
    snprintf(timestamp, sizeof(timestamp), "%lld", (long long)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000));

    // 初始化tmpfs文件路径名和dirty-map
    snprintf(dti->dirty_map_path, sizeof(dti->dirty_map_path), "%s/%d-%s.img", tmpfs_dir, pid, timestamp);
    xa_init(&dti->dirty_xarray);

    dti->track_worker = kthread_run(wp_fault_track, dti, "track_worker_%d", pid);
    if (IS_ERR(dti->track_worker)) {
        err = PTR_ERR(dti->track_worker);
        printk(KERN_ERR "Failed to create tracking thread for PID %d: %d\n", pid, err);
        xa_destroy(&dti->dirty_xarray);
        mmput(dti->mm);
        kfree(dti);
        return err;
    }

    // 将dti添加到dirty_track_list链表
    write_lock(&dirty_track_rwlock);
    list_add(&dti->list, &dirty_track_list);
    write_unlock(&dirty_track_rwlock);

    // 增加跟踪进程计数
    atomic_inc(&tracked_processes);

    printk(KERN_INFO "Started dirty-tracking PID %d\n", pid);
    return 0;
}

// 停止并清理对指定PID进程的脏页追踪(内核缓存)
static int stop_dirty_track(pid_t pid) {
    dirty_track_t *dti, *tmp;
    wqtask_completion_t *wqtc;
    nbstop_kthread_t *sw;

    // 不存在进程的脏页追踪
    if (atomic_read(&tracked_processes) == 0 || list_empty(&dirty_track_list)) {
        printk(KERN_ERR "No active dirty-tracking\n");
        return -ENOENT;
    }

    write_lock(&dirty_track_rwlock);
    list_for_each_entry_safe(dti, tmp, &dirty_track_list, list) {
        if (dti->pid == pid) {
            list_del(&dti->list);
            write_unlock(&dirty_track_rwlock);
        
            // 分配完成通知结构体
            wqtc = kzalloc(sizeof(*wqtc), GFP_KERNEL);
            if (!wqtc)
                return -ENOMEM;
            init_completion(&wqtc->wq_comp);

            // 分配队列工作项结构体
            sw = kzalloc(sizeof(*sw), GFP_KERNEL);
            if (!sw) {
                kfree(wqtc);
                return -ENOMEM;
            }
            sw->wq_comp = wqtc;
            sw->dti = dti;
            INIT_WORK(&sw->work, nbstop_kthread_fn);
            list_add_tail(&wqtc->list, &wqtask_completion_list);

            queue_work(nbstop_kthread_wq, &sw->work);
            // 等待工作队列任务完成
            wait_for_completion(&wqtc->wq_comp);

            // 停止内核线程工作完成后的清理工作
            list_del(&wqtc->list);
            kfree(wqtc);

            // 减少跟踪进程计数
            atomic_dec(&tracked_processes);
            printk(KERN_INFO "[3]Successfully stopped monitoring PID %d\n", pid);
            break;
        }
    }
    // 如果没有找到匹配的PID，仍需释放自旋锁
    if (!dti) {
        write_unlock(&dirty_track_rwlock);
    }

    return 0;
}

// ioctl 处理函数
static long device_ioctl(struct file *file, unsigned int cmd, unsigned long arg) {
    pid_t pid;
    char user_path[256];

    // 权限检查：仅允许有CAP_SYS_ADMIN权限的进程操作
    if (!capable(CAP_SYS_ADMIN)) {
        return -EPERM;
    }

    switch (cmd) {
        case IOCTL_SET_DIRTY_MAP_PATH:
            if (copy_from_user(user_path, (char __user *)arg, sizeof(user_path))) {
                return -EFAULT;
            }
            // 字符串长度和内容验证
            if (strnlen(user_path, sizeof(user_path)) >= sizeof(user_path)) {
                printk(KERN_ERR "Invalid dirty_map_path length.\n");
                return -EINVAL;
            }
            // 复制路径到tmpfs_dir
            strncpy(tmpfs_dir, user_path, sizeof(tmpfs_dir) - 1);
            tmpfs_dir[sizeof(tmpfs_dir) - 1] = '\0'; // 确保终止
            printk(KERN_INFO "dirty-map's dir is set to %s\n", tmpfs_dir);
            break;
        case IOCTL_START_PID:
            if (copy_from_user(&pid, (pid_t __user *)arg, sizeof(pid_t))) {
                return -EFAULT;
            }
            return start_dirty_track(pid);
        case IOCTL_STOP_PID:
            if (copy_from_user(&pid, (pid_t __user *)arg, sizeof(pid_t))) {
                return -EFAULT;
            }
            return stop_dirty_track(pid);
        case IOCTL_GET_DIRTY_MAP_PATH:
            if (strnlen(tmpfs_dir, sizeof(tmpfs_dir)) >= sizeof(user_path)) {
                return -EINVAL;
            }
            if (copy_to_user((char __user *)arg, &tmpfs_dir, strlen(tmpfs_dir) + 1)) {
                return -EFAULT;
            }
            break;
        default:
            return -EINVAL;
    }

    return 0;
}

// 文件操作函数实现（即使是空实现也要有）
static int device_open(struct inode *inode, struct file *file) {
    return 0;
}

static int device_release(struct inode *inode, struct file *file) {
    return 0;
}

// 文件操作结构体
static struct file_operations fops = {
    .unlocked_ioctl = device_ioctl,
    .owner = THIS_MODULE,
    .open = device_open,
    .release = device_release,
};

// 模块初始化
static int __init lkm_init(void) {
    int ret, err;

    // 初始化全局锁
    rwlock_init(&dirty_track_rwlock);

    // 动态分配主设备号并创建dirty-track字符设备
    ret = alloc_chrdev_region(&dev, 0, 1, DEVICE_NAME);
    if (ret < 0) {
        printk(KERN_ALERT "Failed to register character device\n");
        return ret;
    }
    printk(KERN_INFO "MAJOR number of dirty-track is %d\n", MAJOR(dev));    //用 MAJOR宏提取主设备号
    printk(KERN_INFO "MINOR number of dirty-track is %d\n", MINOR(dev));    //用 MINOR宏提取此设备号

    // 初始化cdev结构并添加到内核
    cdev_init(&dirty_track_cdev, &fops);
    dirty_track_cdev.owner = THIS_MODULE;

    ret = cdev_add(&dirty_track_cdev, dev, 1);
    if (ret < 0) {
        unregister_chrdev_region(dev, 1);
        printk(KERN_ALERT "Failed to add cdev for dirty-track\n");
        return ret;
    }

    // 创建dirty-track类和dev设备节点
    dirty_track_class = class_create(THIS_MODULE, "dirty_track_class");
    if (IS_ERR(dirty_track_class)) {
        unregister_chrdev_region(dev, 1);
        printk(KERN_ALERT "Failed to create dirty_track_class\n");
        return PTR_ERR(dirty_track_class);
    }
    
    // device_create(dirty_track_class, NULL, dev, NULL, DEVICE_NAME);
    // 创建设备节点
    if (device_create(dirty_track_class, NULL, dev, NULL, DEVICE_NAME) == NULL) {
        class_destroy(dirty_track_class);
        unregister_chrdev_region(dev, 1);
        printk(KERN_ALERT "Failed to create the dirty-track device\n");
        return -1;
    }

    // 初始化非阻塞停止内核线程的工作队列
    nbstop_kthread_wq = create_workqueue("nbstop_kthread_wq");
    if (!nbstop_kthread_wq) {
        class_destroy(dirty_track_class);
        unregister_chrdev_region(dev, 1);
        return -ENOMEM;
    }

    printk(KERN_INFO "dirty-track LKM initialized\n");
    return 0;
}

// 模块卸载
static void __exit lkm_exit(void) {
    dirty_track_t *dti, *tmp;

    // 停止所有脏页追踪实例
    write_lock(&dirty_track_rwlock);
    list_for_each_entry_safe(dti, tmp, &dirty_track_list, list) {
        list_del(&dti->list);
        write_unlock(&dirty_track_rwlock);
        
        // 分配工作队列完成通知结构体
        wqtask_completion_t *wqtc = kzalloc(sizeof(*wqtc), GFP_KERNEL);
        if (!wqtc) {
            printk(KERN_ERR "Failed to allocate wqtask_completion_t during exit\n");
            // 继续尝试清理其他实例
            write_lock(&dirty_track_rwlock);
            continue;
        }
        init_completion(&wqtc->wq_comp);

        // 分配并初始化工作队列任务结构体
        nbstop_kthread_t *sw = kzalloc(sizeof(*sw), GFP_KERNEL);
        if (!sw) {
            printk(KERN_ERR "Failed to allocate nbstop_kthread_t during exit\n");
            kfree(wqtc);
            write_lock(&dirty_track_rwlock);
            continue;
        }
        sw->dti = dti;
        sw->wq_comp = wqtc;
        INIT_WORK(&sw->work, nbstop_kthread_fn);

        // 将完成通知结构体添加到任务完成链表
        list_add_tail(&wqtc->list, &wqtask_completion_list);

        // 将工作队列任务推送到工作队列
        queue_work(nbstop_kthread_wq, &sw->work);

        // 等待工作队列任务完成
        wait_for_completion(&wqtc->wq_comp);

        // 移除完成通知结构体并释放内存
        list_del(&wqtc->list);
        kfree(wqtc);
        
        // 减少跟踪进程计数
        atomic_dec(&tracked_processes);
        write_lock(&dirty_track_rwlock);
    }
    write_unlock(&dirty_track_rwlock);

    // 等待所有工作队列清空
    flush_workqueue(nbstop_kthread_wq);

    // 销毁所有工作队列
    destroy_workqueue(nbstop_kthread_wq);

    // 卸载顺序和创建顺序相反(栈)
    cdev_del(&dirty_track_cdev);
    device_destroy(dirty_track_class, dev);
    class_destroy(dirty_track_class);
    unregister_chrdev_region(dev, 1);
    printk(KERN_INFO "dirty-track LKM unloaded\n");
}

module_init(lkm_init);
module_exit(lkm_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Obsidian");
MODULE_DESCRIPTION("A kernel module to track page write-faults for multiple processes");