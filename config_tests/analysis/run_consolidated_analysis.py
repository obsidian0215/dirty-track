#!/usr/bin/env python3
"""Consolidated analysis entrypoint.

This script performs the common analysis pipeline in one file:
  - aggregate CRIU dump `analysis.json` files into `results/full_run_summary.csv`
  - fill missing `size_distribution` values when pattern looks like a distribution
  - generate combo-only plots under `results/plots_combo/`
  - run a simple linear model (least-squares) and write `results/model_summary.txt`
  - print top combo labels

It replaces multiple smaller analysis scripts by providing the core
functionality in a single place. Delete or ignore other per-purpose scripts.

By default the script will use an existing `results/full_run_summary.csv` if
present to avoid re-scanning dump directories. Use `--force` (or delete the
CSV) to force re-aggregation. The script also auto-detects when new dump
analysis files ("*_dump/analysis.json") exist that are not present in the
current CSV and will re-aggregate automatically in that case.
"""
import os
import sys
import json
import csv
import re
import statistics
import argparse
from collections import defaultdict, Counter

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RESULTS = os.path.join(ROOT, 'results')
os.makedirs(RESULTS, exist_ok=True)
PLOTS_DIR = os.path.join(RESULTS, 'plots_combo')
os.makedirs(PLOTS_DIR, exist_ok=True)


def parse_dirname(d):
    name = os.path.basename(d)
    if name.endswith('_dump'):
        name = name[:-5]
    parts = name.split('__')
    first = parts[0]
    if '_' in first:
        container, test = first.split('_', 1)
    else:
        container, test = first, ''
    params = {'container': container, 'test': test}
    for p in parts[1:]:
        if not p:
            continue
        if p.startswith('fr') and p[2:].isdigit():
            params['framerate'] = int(p[2:])
        elif p.startswith('r') and p[1:].isdigit():
            params['rps'] = int(p[1:])
        elif p.startswith('pat'):
            params['pattern'] = p[3:]
        elif p.startswith('p'):
            v = p[1:]
            try:
                if v.lower().endswith('kb'):
                    params['payload'] = int(float(v[:-2]) * 1024)
                elif v.lower().endswith('mb'):
                    params['payload'] = int(float(v[:-2]) * 1024 * 1024)
                else:
                    params['payload'] = int(v)
            except Exception:
                params['payload'] = v
        elif p.startswith('m'):
            params['payload_mode'] = p[1:]
        elif p.startswith('sd'):
            params['size_distribution'] = p[2:]
        elif p.startswith('vp'):
            params['vehicle_pattern'] = p[2:]
        elif p.startswith('spd'):
            try:
                params['sensors_per_device'] = int(p[3:])
            except Exception:
                params['sensors_per_device'] = p[3:]
        elif p.startswith('th'):
            try:
                params['threads'] = int(p[2:])
            except Exception:
                params['threads'] = p[2:]
        elif p.startswith('d') and p[1:].isdigit():
            params['duration'] = int(p[1:])
        elif p.startswith('res'):
            params['resolution'] = p[3:]
        else:
            params[p] = True
    return params


def aggregate_results():
    rows = []
    for name in sorted(os.listdir(RESULTS)):
        if not name.endswith('_dump'):
            continue
        d = os.path.join(RESULTS, name)
        if not os.path.isdir(d):
            continue
        analysis_path = os.path.join(d, 'analysis.json')
        if not os.path.isfile(analysis_path):
            continue
        try:
            with open(analysis_path, 'r', encoding='utf-8') as f:
                j = json.load(f)
        except Exception as e:
            print('failed to load', analysis_path, e)
            continue
        meta = parse_dirname(d)
        row = dict(meta)
        row['dump_dir'] = d
        base = j.get('base') or {}
        row.update({
            'page_size': j.get('page_size'),
            'total_tracked_pages': j.get('total_tracked_pages'),
            'total_writes': j.get('total_writes'),
            'total_pages': base.get('total_pages'),
            'data_pages': base.get('data_pages'),
            'zero_pages': base.get('zero_pages'),
            'constant_pages': base.get('constant_pages'),
            'avg_entropy': base.get('avg_entropy'),
            'avg_zero_ratio': base.get('avg_zero_ratio'),
            'unique_hashes': base.get('unique_hashes'),
            'duplicate_pages': base.get('duplicate_pages'),
        })
        rows.append(row)

    # normalize
    keys = ['container','test','rps','framerate','payload','payload_mode','pattern','size_distribution','threads','duration','resolution','page_size','total_tracked_pages','total_pages','data_pages','zero_pages','constant_pages','avg_entropy','avg_zero_ratio','unique_hashes','duplicate_pages','dump_dir']
    numeric_keys = set(['rps','framerate','payload','threads','duration','page_size','total_tracked_pages','total_pages','data_pages','zero_pages','constant_pages','avg_entropy','avg_zero_ratio','unique_hashes','duplicate_pages'])
    for r in rows:
        for k in keys:
            v = r.get(k, '')
            if v is None or v == '':
                r[k] = 0 if k in numeric_keys else 'NA'
        if not r.get('dump_dir'):
            r['dump_dir'] = 'NA'

    out_csv = os.path.join(RESULTS, 'full_run_summary.csv')
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in keys})
    print('Wrote summary to', out_csv, len(rows), 'rows')
    return out_csv


def fill_size_distribution(csv_path):
    import shutil
    from pathlib import Path
    src = Path(csv_path)
    bak = src.with_suffix('.csv.bak2')
    if not src.exists():
        print('CSV not found:', src)
        return False
    shutil.copy2(src, bak)
    print('Backed up', src, '->', bak)
    rows = []
    with src.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        fieldnames = r.fieldnames
        for row in r:
            rows.append(row)
    dist_candidates = set(['uniform','normal','zipf','random'])
    changed = 0
    for row in rows:
        sd = row.get('size_distribution','')
        pat = (row.get('pattern') or '').strip()
        if (not sd or sd=='NA') and pat and pat.lower() in dist_candidates:
            row['size_distribution'] = pat
            changed += 1
    with src.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print('Wrote', src, '- filled', changed, 'rows')
    return True


def make_plots(csv_path):
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import LogLocator, FuncFormatter

    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    if not rows:
        print('no rows in CSV')
        return False

    def sf(v):
        try:
            if v is None or v=='' or (isinstance(v, float) and np.isnan(v)):
                return float('nan')
            return float(v)
        except Exception:
            return float('nan')

    # build combos
    combos = []
    for r in rows:
        cont = r.get('container') or 'NA'
        test = r.get('test') or 'NA'
        pm = r.get('payload_mode') or 'NA'
        pat = r.get('pattern') or 'NA'
        sd = r.get('size_distribution') or 'NA'
        payload = r.get('payload')
        try:
            pstr = str(int(float(payload))) if payload not in (None,'') else 'NA'
        except Exception:
            pstr = str(payload) if payload and str(payload).strip() else 'NA'
        rps = r.get('rps')
        try:
            rpss = str(int(float(rps))) if rps not in (None,'') else 'NA'
        except Exception:
            rpss = 'NA'
        dist = sd if sd and sd != 'NA' else pat or 'NA'
        lab = f"{cont}/{test}|{pm}|{dist}|p{pstr}|r{rpss}"
        combos.append((lab, r))

    stats = {}
    for lab, r in combos:
        tp = sf(r.get('total_pages'))
        zp = sf(r.get('zero_pages'))
        dp = sf(r.get('duplicate_pages'))
        cp = sf(r.get('constant_pages'))
        ps = sf(r.get('page_size'))
        azr = sf(r.get('avg_zero_ratio'))
        unique_pages = tp - dp if not np.isnan(tp) and not np.isnan(dp) else float('nan')
        stats.setdefault(lab, []).append({'total': tp, 'zero': zp, 'dup': dp, 'const': cp, 'unique': unique_pages, 'page_size': ps, 'avg_zero_ratio': azr})

    combo_list = []
    for lab, items in stats.items():
        totals = np.array([it['total'] for it in items], dtype=float)
        zeros = np.array([it['zero'] for it in items], dtype=float)
        dups = np.array([it['dup'] for it in items], dtype=float)
        consts = np.array([it['const'] for it in items], dtype=float)
        pageszs = np.array([it.get('page_size', float('nan')) for it in items], dtype=float)
        avg_zrs = np.array([it.get('avg_zero_ratio', float('nan')) for it in items], dtype=float)
        def mean_safe(a):
            if np.all(np.isnan(a)):
                return float('nan')
            return np.nanmean(a)
        m_total = mean_safe(totals)
        m_zero = mean_safe(zeros)
        m_dup = mean_safe(dups)
        m_const = mean_safe(consts)
        m_pagesz = mean_safe(pageszs)
        m_avgzr = mean_safe(avg_zrs)
        if not np.isnan(m_total) and m_total > 0:
            z = 0.0 if np.isnan(m_zero) else float(m_zero)
            c_nonzero = max(0.0, (0.0 if np.isnan(m_const) else float(m_const)) - z)
            d_non = max(0.0, (0.0 if np.isnan(m_dup) else float(m_dup)) - z - c_nonzero)
            rem = max(0.0, m_total - (z + c_nonzero + d_non))
            prop_zero = z / m_total
            prop_const = c_nonzero / m_total
            prop_dup = d_non / m_total
            prop_uniq_non = rem / m_total
        else:
            prop_zero = prop_dup = prop_const = prop_uniq_non = 0.0
        combo_list.append({'lab': lab, 'total': m_total, 'prop_zero': prop_zero, 'prop_dup': prop_dup, 'prop_const': prop_const, 'prop_uniq_non': prop_uniq_non, 'count': len(items), 'mean_page_size': m_pagesz, 'mean_avg_zero_ratio': m_avgzr})

    combo_list.sort(key=lambda x: (0 if np.isnan(x['total']) else x['total']), reverse=True)
    TOP = 40
    top = combo_list[:TOP]
    labels = [c['lab'] for c in top]
    totals = [0 if np.isnan(c['total']) else c['total'] for c in top]
    zeros = [c['prop_zero'] for c in top]
    dups = [c['prop_dup'] for c in top]
    consts = [c['prop_const'] for c in top]
    uniqs = [c['prop_uniq_non'] for c in top]

    # space out bars for readability
    base_y = np.arange(len(labels))
    gap = 1.4
    y = base_y * gap
    workloads = [l.split('|', 1)[0] for l in labels]
    unique_w = sorted(set(workloads))
    cmap = plt.get_cmap('tab20')
    wcolor_map = {w: cmap(i % 20) for i, w in enumerate(unique_w)}

    fig, ax = plt.subplots(figsize=(14, max(8, len(labels)*0.3)))
    seg_colors = {'zero': '#6baed6', 'dup': '#fd8d3c', 'uniq': '#74c476', 'const': '#9e9ac8'}
    bar_h = 0.9
    # draw faint alternating row backgrounds for clarity
    for i in range(len(labels)):
        if i % 2 == 0:
            ax.axhspan(y[i]-bar_h/2, y[i]+bar_h/2, color='#fafafa', zorder=0)
        wc = wcolor_map.get(workloads[i], (0.5,0.5,0.5))
        left = 0.0
        ax.barh(y[i], zeros[i], left=left, color=seg_colors['zero'], edgecolor=wc, linewidth=0.8, height=bar_h)
        left += zeros[i]
        ax.barh(y[i], dups[i], left=left, color=seg_colors['dup'], edgecolor=wc, linewidth=0.8, height=bar_h)
        left += dups[i]
        ax.barh(y[i], uniqs[i], left=left, color=seg_colors['uniq'], edgecolor=wc, linewidth=0.8, height=bar_h)
        left += uniqs[i]
        ax.barh(y[i], consts[i], left=left, color=seg_colors['const'], edgecolor=wc, linewidth=0.8, height=bar_h)
        ax.barh(y[i], 0.01, left=-0.02, color=wc, edgecolor=wc, height=bar_h)
    # make labels sparser if there are many
    step = max(1, len(labels) // 30)
    sparse_labels = [lab if (i % step) == 0 else '' for i, lab in enumerate(labels)]
    ax.set_yticks(y)
    ax.set_yticklabels(sparse_labels, fontsize=9)
    ax.set_xlabel('fraction of total_pages')
    ax.set_title('Page-type proportions by combo (top %d combos by mean total_pages)' % TOP)
    seg_handles = [plt.Rectangle((0,0),1,1,color=seg_colors[k]) for k in ('zero','dup','uniq','const')]
    seg_labels = ['zero_pages','duplicate_pages','unique_nonzero_nonconst','constant_pages']
    ax.legend(seg_handles, seg_labels, loc='lower right')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'page_type_proportions_by_combo_top%d.png' % TOP))
    plt.close()

    fig, ax = plt.subplots(figsize=(16, max(8, len(labels)*0.3)))
    bar_h_tot = 0.9
    for i in range(len(labels)):
        if i % 2 == 0:
            ax.axhspan(y[i]-bar_h_tot/2, y[i]+bar_h_tot/2, color='#fafafa', zorder=0)
    ax.barh(y, totals, color=[plt.get_cmap('tab20')(i % 20) for i in range(len(labels))], height=bar_h_tot)
    step_tot = max(1, len(labels) // 30)
    sparse_labels_tot = [lab if (i % step_tot) == 0 else '' for i, lab in enumerate(labels)]
    ax.set_yticks(y)
    ax.set_yticklabels(sparse_labels_tot, fontsize=10)
    ax.set_xlabel('mean total_pages')
    clean_totals = [v for v in totals if v and not np.isnan(v) and v > 0]
    if clean_totals:
        mx = max(clean_totals)
        mn = min(clean_totals)
        if mn > 0 and mx / mn > 50:
            ax.set_xscale('log')
            ax.xaxis.set_major_locator(LogLocator(base=10.0))
            def _logfmt(x, pos):
                try:
                    if x >= 1000:
                        return f"{int(x/1000)}k"
                    return f"{int(x)}"
                except Exception:
                    return str(x)
            ax.xaxis.set_major_formatter(FuncFormatter(_logfmt))
        else:
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x):,}"))
    ax.set_title('Mean total_pages by combo (top %d)' % TOP)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'mean_total_pages_by_combo_top%d.png' % TOP))
    plt.close()
    print('Saved combo plots to', PLOTS_DIR)

    # Split into two separate figures per user request:
    # 1) combined memory for all combos
    # 2) page-type proportions for all combos (with avg_zero overlay)
    all_labels = [c['lab'] for c in combo_list]
    mems = []
    prop_zeros = []
    prop_dups = []
    prop_consts = []
    prop_uniqs = []
    avg_zero_bytes = []
    for c in combo_list:
        tp = c.get('total')
        ps = c.get('mean_page_size')
        if tp is None or ps is None or ps == 0 or tp == 0 or np.isnan(tp) or np.isnan(ps):
            mems.append(float('nan'))
        else:
            mems.append((float(tp) * float(ps)) / (1024.0*1024.0))
        prop_zeros.append(c.get('prop_zero') or 0.0)
        prop_dups.append(c.get('prop_dup') or 0.0)
        prop_consts.append(c.get('prop_const') or 0.0)
        prop_uniqs.append(c.get('prop_uniq_non') or 0.0)
        avg_zero_bytes.append(c.get('mean_avg_zero_ratio') or 0.0)


    N = len(all_labels)
    base_all = np.arange(N)
    gap_all = 1.6
    y_all = base_all * gap_all

    # Memory-only figure (larger, sparser labels)
    fig_mem, axm = plt.subplots(figsize=(18, max(8, N * 0.22)))
    bar_h_mem = 1.0
    for i in range(N):
        if i % 2 == 0:
            axm.axhspan(y_all[i]-bar_h_mem/2, y_all[i]+bar_h_mem/2, color='#fafafa', zorder=0)
    axm.barh(y_all, mems, color=[plt.get_cmap('tab20')(i % 20) for i in range(N)], height=bar_h_mem)
    step_mem = max(1, N // 30)
    sparse_labels_mem = [lab if (i % step_mem) == 0 else '' for i, lab in enumerate(all_labels)]
    axm.set_yticks(y_all)
    axm.set_yticklabels(sparse_labels_mem, fontsize=10)
    axm.invert_yaxis()
    axm.set_xlabel('memory (MB)')
    axm.set_title('Estimated memory (MB) by combo (all combos)')
    plt.tight_layout()
    out_mem = os.path.join(PLOTS_DIR, 'combined_memory_all.png')
    plt.savefig(out_mem)
    plt.close()
    print('Saved memory comparison to', out_mem)

    # Page-type proportions figure (larger, sparser labels)
    fig_pages, axp = plt.subplots(figsize=(18, max(10, N * 0.28)))
    bar_h_pages = 1.0
    for i in range(N):
        if i % 2 == 0:
            axp.axhspan(y_all[i]-bar_h_pages/2, y_all[i]+bar_h_pages/2, color='#fafafa', zorder=0)
    axp.invert_yaxis()
    axp.barh(y_all, prop_zeros, color='#6baed6', label='zero_pages', height=bar_h_pages)
    axp.barh(y_all, prop_dups, left=prop_zeros, color='#fd8d3c', label='duplicate_pages', height=bar_h_pages)
    left2 = [a+b for a,b in zip(prop_zeros, prop_dups)]
    axp.barh(y_all, prop_consts, left=left2, color='#9e9ac8', label='constant_pages', height=bar_h_pages)
    left3 = [a+b+c for a,b,c in zip(prop_zeros, prop_dups, prop_consts)]
    uniqs = [max(0.0, 1.0 - (a+b+c)) for a,b,c in zip(prop_zeros, prop_dups, prop_consts)]
    axp.barh(y_all, uniqs, left=left3, color='#74c476', label='unique_nonzero_nonconst', height=bar_h_pages)
    step_pages = max(1, N // 30)
    sparse_labels_pages = [lab if (i % step_pages) == 0 else '' for i, lab in enumerate(all_labels)]
    axp.set_yticks(y_all)
    axp.set_yticklabels(sparse_labels_pages, fontsize=10)
    axp.set_xlabel('fraction of pages')
    axp.set_title('Page-type proportions by combo (all combos)')
    axp.legend(loc='lower right')

    # overlay avg_zero_bytes as a red dot on the fraction axis
    try:
        axt = axp.twiny()
        axt.plot(avg_zero_bytes, x, 'ro', markersize=3, label='avg_zero_ratio (byte-level)')
        axt.set_xlim(0, 1)
        axt.set_xlabel('avg_zero_ratio (bytes)')
        axt.legend(loc='upper right')
    except Exception:
        pass

    plt.tight_layout()
    out_pages = os.path.join(PLOTS_DIR, 'combined_page_types_all.png')
    plt.savefig(out_pages)
    plt.close()
    print('Saved page-type proportions to', out_pages)
    # Also produce a separate avg_zero_ratio plot (zero-byte fraction per combo)
    try:
        fig_z, axz = plt.subplots(figsize=(18, max(6, N * 0.18)))
        bar_h_z = 1.0
        for i in range(N):
            if i % 2 == 0:
                axz.axhspan(y_all[i]-bar_h_z/2, y_all[i]+bar_h_z/2, color='#fafafa', zorder=0)
        axz.barh(y_all, avg_zero_bytes, color='#d73027', height=bar_h_z)
        step_z = max(1, N // 30)
        sparse_labels_z = [lab if (i % step_z) == 0 else '' for i, lab in enumerate(all_labels)]
        axz.set_yticks(y_all)
        axz.set_yticklabels(sparse_labels_z, fontsize=10)
        axz.invert_yaxis()
        axz.set_xlabel('avg_zero_ratio (bytes)')
        axz.set_xlim(0, 1)
        axz.set_title('Average zero-byte ratio (by combo)')
        plt.tight_layout()
        out_zero = os.path.join(PLOTS_DIR, 'combined_avg_zero_ratio_all.png')
        plt.savefig(out_zero)
        plt.close()
        print('Saved avg_zero_ratio plot to', out_zero)
    except Exception:
        print('Failed to save avg_zero_ratio plot')
    return True


def analyze_models(csv_path):
    try:
        import numpy as np
    except Exception:
        print('numpy required for modeling; skipping')
        return False
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    def to_float(x):
        try:
            if x is None or x == '':
                return None
            return float(x)
        except Exception:
            return None
    target = []
    Xnum = []
    cat_payload_mode = []
    cat_pattern = []
    for r in rows:
        tp = to_float(r.get('total_pages'))
        if tp is None:
            continue
        rps = to_float(r.get('rps')) or 0.0
        threads = to_float(r.get('threads')) or 0.0
        duration = to_float(r.get('duration')) or 0.0
        payload = to_float(r.get('payload')) or 0.0
        target.append(tp)
        Xnum.append([rps, threads, duration, payload])
        cat_payload_mode.append(r.get('payload_mode') or '')
        cat_pattern.append(r.get('pattern') or '')
    if len(target) < 5:
        print('Not enough data for modelling')
        return False
    Y = np.array(target)
    Xn = np.array(Xnum)
    from collections import Counter
    def one_hot_top(values, k=4):
        c = Counter(values)
        topk = [t for t,_ in c.most_common(k)]
        mat = np.zeros((len(values), len(topk)), dtype=float)
        for i,v in enumerate(values):
            if v in topk:
                j = topk.index(v)
                mat[i,j] = 1.0
        return mat, topk
    Pmat, Ptop = one_hot_top(cat_payload_mode, k=3)
    Patmat, Pattop = one_hot_top(cat_pattern, k=6)
    X = np.hstack([np.ones((Xn.shape[0],1)), Xn, Pmat, Patmat])
    coef, *_ = np.linalg.lstsq(X, Y, rcond=None)
    Yhat = X.dot(coef)
    ss_res = np.sum((Y - Yhat)**2)
    ss_tot = np.sum((Y - np.mean(Y))**2)
    r2 = 1 - ss_res/ss_tot if ss_tot>0 else 0.0
    out = os.path.join(RESULTS, 'model_summary.txt')
    with open(out, 'w', encoding='utf-8') as f:
        f.write('Model: total_pages ~ rps + threads + duration + payload + payload_mode(top) + pattern(top)\n')
        f.write('R2: {:.4f}\n'.format(r2))
        f.write('\nCoefficients (intercept, rps, threads, duration, payload, payload_mode top {}, pattern top {}):\n'.format(Ptop, Pattop))
        for i,c in enumerate(coef):
            f.write('{:3d}: {:.6g}\n'.format(i, float(c)))
        names = ['intercept','rps','threads','duration','payload'] + ['pm_'+p for p in Ptop] + ['pat_'+p for p in Pattop]
        f.write('\nTop coefficients by absolute value:\n')
        abscoef = [(abs(float(coef[i])), names[i]) for i in range(len(names))]
        abscoef.sort(reverse=True)
        for v,nm in abscoef[:10]:
            f.write('{:12.6g}  {}\n'.format(v,nm))
    print('Wrote model summary to', out)
    return True


def make_comparison_bars(csv_path, top=40):
    """Create comparison bar charts for memory size and page/byte proportions.

    Generates:
      - `plots_combo/comparison_page_proportions_topN.png` (stacked bar: zero/dup/const/unique)
      - `plots_combo/comparison_memory_topN.png` (bar: memory MB)
    """
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    if not rows:
        print('no rows for comparison bars')
        return False

    def sf(v):
        try:
            if v is None or v == '' or v == 'NA':
                return float('nan')
            return float(v)
        except Exception:
            return float('nan')

    combos = {}
    for r in rows:
        cont = r.get('container') or 'NA'
        test = r.get('test') or 'NA'
        pm = r.get('payload_mode') or 'NA'
        pat = r.get('pattern') or 'NA'
        sd = r.get('size_distribution') or 'NA'
        payload = r.get('payload')
        try:
            pstr = str(int(float(payload))) if payload not in (None,'') else 'NA'
        except Exception:
            pstr = str(payload) if payload and str(payload).strip() else 'NA'
        rps = r.get('rps')
        try:
            rpss = str(int(float(rps))) if rps not in (None,'') else 'NA'
        except Exception:
            rpss = 'NA'
        dist = sd if sd and sd != 'NA' else pat or 'NA'
        lab = f"{cont}/{test}|{pm}|{dist}|p{pstr}|r{rpss}"
        combos.setdefault(lab, []).append(r)

    stats = []
    for lab, items in combos.items():
        totals = np.array([sf(it.get('total_pages')) for it in items], dtype=float)
        pagesz = np.array([sf(it.get('page_size')) for it in items], dtype=float)
        zeros = np.array([sf(it.get('zero_pages')) for it in items], dtype=float)
        dups = np.array([sf(it.get('duplicate_pages')) for it in items], dtype=float)
        consts = np.array([sf(it.get('constant_pages')) for it in items], dtype=float)
        avg_zero_ratio = np.array([sf(it.get('avg_zero_ratio')) for it in items], dtype=float)
        def mean_safe(a):
            if a.size == 0 or np.all(np.isnan(a)):
                return float('nan')
            return float(np.nanmean(a))
        m_total = mean_safe(totals)
        m_pagesz = mean_safe(pagesz)
        m_zero = mean_safe(zeros)
        m_dup = mean_safe(dups)
        m_const = mean_safe(consts)
        m_avgzr = mean_safe(avg_zero_ratio)
        mem_mb = float('nan')
        if not np.isnan(m_total) and not np.isnan(m_pagesz):
            mem_mb = (m_total * m_pagesz) / (1024.0*1024.0)
        prop_zero_pages = (m_zero / m_total) if (not np.isnan(m_zero) and not np.isnan(m_total) and m_total>0) else float('nan')
        prop_dup_pages = (m_dup / m_total) if (not np.isnan(m_dup) and not np.isnan(m_total) and m_total>0) else float('nan')
        prop_const_pages = (m_const / m_total) if (not np.isnan(m_const) and not np.isnan(m_total) and m_total>0) else float('nan')
        # estimate zero-bytes fraction using avg_zero_ratio (per-page average)
        prop_zero_bytes = m_avgzr if not np.isnan(m_avgzr) else float('nan')
        prop_other_bytes = 1.0 - prop_zero_bytes if not np.isnan(prop_zero_bytes) else float('nan')
        stats.append({'lab': lab, 'mem_mb': mem_mb, 'prop_zero_pages': prop_zero_pages, 'prop_dup_pages': prop_dup_pages, 'prop_const_pages': prop_const_pages, 'prop_zero_bytes': prop_zero_bytes, 'prop_other_bytes': prop_other_bytes, 'total_mean_pages': m_total})

    stats.sort(key=lambda x: (0 if np.isnan(x['total_mean_pages']) else x['total_mean_pages']), reverse=True)
    TOP = min(top, len(stats))
    sel = stats[:TOP]
    labels = [s['lab'] for s in sel]

    # stacked bar (page-type proportions)
    zeros = [0.0 if np.isnan(s['prop_zero_pages']) else s['prop_zero_pages'] for s in sel]
    dups = [0.0 if np.isnan(s['prop_dup_pages']) else s['prop_dup_pages'] for s in sel]
    consts = [0.0 if np.isnan(s['prop_const_pages']) else s['prop_const_pages'] for s in sel]
    uniqs = [max(0.0, 1.0 - (z + d + c)) for z,d,c in zip(zeros, dups, consts)]

    x = np.arange(len(labels))
    width = 0.8
    fig, ax = plt.subplots(figsize=(10, max(4, len(labels)*0.18)))
    ax.bar(x, zeros, width, label='zero_pages', color='#6baed6')
    ax.bar(x, dups, width, bottom=zeros, label='duplicate_pages', color='#fd8d3c')
    bottom2 = [a+b for a,b in zip(zeros, dups)]
    ax.bar(x, consts, width, bottom=bottom2, label='constant_pages', color='#9e9ac8')
    bottom3 = [a+b+c for a,b,c in zip(zeros, dups, consts)]
    ax.bar(x, uniqs, width, bottom=bottom3, label='unique_nonzero_nonconst', color='#74c476')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel('fraction of pages')
    ax.set_title(f'Page-type proportions by combo (top {TOP})')
    ax.legend()
    plt.tight_layout()
    out1 = os.path.join(PLOTS_DIR, f'comparison_page_proportions_top{TOP}.png')
    plt.savefig(out1)
    plt.close()
    print('Saved page proportions comparison to', out1)

    # memory bar
    mems = [0.0 if np.isnan(s['mem_mb']) else s['mem_mb'] for s in sel]
    fig, ax = plt.subplots(figsize=(10, max(4, len(labels)*0.12)))
    ax.barh(x, mems, color=[plt.get_cmap('tab20')(i % 20) for i in range(len(labels))])
    ax.set_yticks(x)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel('memory (MB)')
    ax.set_title(f'Mean memory (MB) by combo (top {TOP})')
    plt.tight_layout()
    out2 = os.path.join(PLOTS_DIR, f'comparison_memory_top{TOP}.png')
    plt.savefig(out2)
    plt.close()
    print('Saved memory comparison to', out2)
    return True


def compute_byte_level_summary(csv_path):
    """Compute byte-level derived metrics and correlations.

    Produces:
      - `full_byte_page_summary.csv` : extended CSV with derived columns
      - `correlation_matrix.csv` : Pearson correlation matrix for numeric features
      - `correlation_summary.txt` : human-readable top correlations
      - `plots_combo/correlation_heatmap.png` : heatmap of correlation matrix
    """
    try:
        import numpy as np
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except Exception:
        print('numpy/matplotlib required for byte-level summary; skipping')
        return False

    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        fieldnames = r.fieldnames
        for row in r:
            rows.append(row)
    if not rows:
        print('no rows in CSV for byte-level summary')
        return False

    # derive fields
    ext_rows = []
    for r in rows:
        def tof(k):
            try:
                v = r.get(k, '')
                if v is None or v == '' or v == 'NA':
                    return float('nan')
                return float(v)
            except Exception:
                return float('nan')
        total_pages = tof('total_pages')
        page_size = tof('page_size')
        avg_zero_ratio = tof('avg_zero_ratio')
        zero_pages = tof('zero_pages')
        payload = tof('payload')
        # estimated zero bytes across tracked pages
        est_zero_bytes = float('nan')
        if not np.isnan(avg_zero_ratio) and not np.isnan(total_pages) and not np.isnan(page_size):
            est_zero_bytes = float(avg_zero_ratio) * float(total_pages) * float(page_size)
        zero_pages_ratio = float('nan')
        if not np.isnan(zero_pages) and not np.isnan(total_pages) and total_pages > 0:
            zero_pages_ratio = zero_pages / total_pages
        nr = dict(r)
        nr['zero_pages_ratio'] = zero_pages_ratio
        nr['est_zero_bytes'] = est_zero_bytes
        ext_rows.append(nr)

    out_extended = os.path.join(RESULTS, 'full_byte_page_summary.csv')
    fnames = list(fieldnames) + ['zero_pages_ratio', 'est_zero_bytes']
    with open(out_extended, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fnames)
        w.writeheader()
        for r in ext_rows:
            w.writerow({k: r.get(k, '') for k in fnames})
    print('Wrote extended byte/page summary to', out_extended)

    # correlation matrix for numeric fields
    numeric_keys = ['total_pages', 'page_size', 'payload', 'rps', 'framerate', 'avg_entropy', 'avg_zero_ratio', 'zero_pages_ratio', 'unique_hashes']
    data = []
    keys_present = []
    for k in numeric_keys:
        keys_present.append(k)
    mat = []
    for r in ext_rows:
        rowvals = []
        for k in keys_present:
            v = r.get(k, '')
            try:
                rowvals.append(float(v))
            except Exception:
                rowvals.append(np.nan)
        mat.append(rowvals)
    A = np.array(mat, dtype=float)
    # drop rows that are all nan
    good = ~np.all(np.isnan(A), axis=1)
    if np.sum(good) < 3:
        print('Not enough numeric rows for correlation')
        return True
    A = A[good]
    # for each column, replace overall nan with column mean
    col_means = np.nanmean(A, axis=0)
    inds = np.where(np.isnan(A))
    A[inds] = np.take(col_means, inds[1])

    try:
        C = np.corrcoef(A, rowvar=False)
    except Exception:
        print('Failed to compute correlation matrix')
        return True

    # save correlation matrix CSV
    out_corr = os.path.join(RESULTS, 'correlation_matrix.csv')
    with open(out_corr, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow([''] + keys_present)
        for i,k in enumerate(keys_present):
            w.writerow([k] + [f"{float(v):.6g}" for v in C[i]])
    print('Wrote correlation matrix to', out_corr)

    # summary: list top absolute correlations (ignore self)
    pairs = []
    n = len(keys_present)
    for i in range(n):
        for j in range(i+1, n):
            pairs.append((abs(C[i,j]), C[i,j], keys_present[i], keys_present[j]))
    pairs.sort(reverse=True, key=lambda x: x[0])
    out_summary = os.path.join(RESULTS, 'correlation_summary.txt')
    with open(out_summary, 'w', encoding='utf-8') as f:
        f.write('Top correlations (abs, signed, key1, key2)\n')
        for v, s, a, b in pairs[:40]:
            f.write(f"{v:.6g}\t{s:.6g}\t{a}\t{b}\n")
    print('Wrote correlation summary to', out_summary)

    # heatmap
    try:
        fig, ax = plt.subplots(figsize=(8, max(4, len(keys_present)*0.4)))
        im = ax.imshow(C, cmap='coolwarm', vmin=-1, vmax=1)
        ax.set_xticks(np.arange(len(keys_present)))
        ax.set_yticks(np.arange(len(keys_present)))
        ax.set_xticklabels(keys_present, rotation=45, ha='right', fontsize=8)
        ax.set_yticklabels(keys_present, fontsize=8)
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        plt.tight_layout()
        out_heat = os.path.join(PLOTS_DIR, 'correlation_heatmap.png')
        plt.savefig(out_heat)
        plt.close()
        print('Saved correlation heatmap to', out_heat)
    except Exception:
        print('Failed to plot correlation heatmap')
    return True


def compute_grouped_correlations(csv_path, min_rows=3):
    """Compute per-workload (combo) correlation matrices and heatmaps.

    Outputs under `results/grouped/`:
      - `<sanitized_combo>_correlation_matrix.csv`
      - `<sanitized_combo>_correlation_heatmap.png`
    Also writes a master summary `correlation_summary_by_group.txt` listing top pairs per group.
    """
    try:
        import numpy as np
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except Exception:
        print('numpy/matplotlib required for grouped correlations; skipping')
        return False

    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    if not rows:
        print('no rows for grouped correlations')
        return False

    def make_combo_label(r):
        cont = r.get('container') or 'NA'
        test = r.get('test') or 'NA'
        pm = r.get('payload_mode') or 'NA'
        pat = r.get('pattern') or 'NA'
        sd = r.get('size_distribution') or 'NA'
        payload = r.get('payload')
        try:
            pstr = str(int(float(payload))) if payload not in (None,'') else 'NA'
        except Exception:
            pstr = str(payload) if payload and str(payload).strip() else 'NA'
        rps = r.get('rps')
        try:
            rpss = str(int(float(rps))) if rps not in (None,'') else 'NA'
        except Exception:
            rpss = 'NA'
        dist = sd if sd and sd != 'NA' else pat or 'NA'
        return f"{cont}/{test}|{pm}|{dist}|p{pstr}|r{rpss}"

    groups = defaultdict(list)
    for r in rows:
        lab = make_combo_label(r)
        groups[lab].append(r)

    outdir = os.path.join(RESULTS, 'grouped')
    os.makedirs(outdir, exist_ok=True)
    master_summary = os.path.join(outdir, 'correlation_summary_by_group.txt')
    with open(master_summary, 'w', encoding='utf-8') as ms:
        ms.write('Group\tN\tTop correlations (abs,signed,key1,key2)\n')
        for lab, items in sorted(groups.items(), key=lambda x: -len(x[1])):
            n = len(items)
            if n < min_rows:
                ms.write(f"{lab}\t{n}\tSKIPPED (n<{min_rows})\n")
                continue
            # build numeric matrix
            numeric_keys = ['total_pages', 'page_size', 'payload', 'rps', 'framerate', 'avg_entropy', 'avg_zero_ratio', 'zero_pages', 'zero_pages_ratio', 'unique_hashes', 'duplicate_pages']
            mat = []
            for r in items:
                rowvals = []
                # ensure zero_pages_ratio exists
                try:
                    zp = float(r.get('zero_pages', 'nan'))
                except Exception:
                    zp = float('nan')
                try:
                    tp = float(r.get('total_pages', 'nan'))
                except Exception:
                    tp = float('nan')
                zr = float('nan')
                if not np.isnan(zp) and not np.isnan(tp) and tp > 0:
                    zr = zp / tp
                r['zero_pages_ratio'] = zr
                for k in numeric_keys:
                    v = r.get(k, '')
                    try:
                        rowvals.append(float(v))
                    except Exception:
                        rowvals.append(np.nan)
                mat.append(rowvals)
            A = np.array(mat, dtype=float)
            # drop rows all nan
            good = ~np.all(np.isnan(A), axis=1)
            A = A[good]
            if A.shape[0] < min_rows:
                ms.write(f"{lab}\t{n}\tSKIPPED_AFTER_NA_FILTER (effective<{min_rows})\n")
                continue
            # fill nan with col mean
            col_means = np.nanmean(A, axis=0)
            inds = np.where(np.isnan(A))
            A[inds] = np.take(col_means, inds[1])
            # corr
            try:
                C = np.corrcoef(A, rowvar=False)
            except Exception:
                ms.write(f"{lab}\t{n}\tFAILED_CORR\n")
                continue
            # save csv
            safe = re.sub(r'[^0-9a-zA-Z_-]', '_', lab)[:180]
            out_csv = os.path.join(outdir, f"{safe}_correlation_matrix.csv")
            with open(out_csv, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow([''] + numeric_keys)
                for i,k in enumerate(numeric_keys):
                    w.writerow([k] + [f"{float(v):.6g}" for v in C[i]])
            # summary top pairs
            pairs = []
            m = len(numeric_keys)
            for i in range(m):
                for j in range(i+1, m):
                    pairs.append((abs(C[i,j]), C[i,j], numeric_keys[i], numeric_keys[j]))
            pairs.sort(reverse=True, key=lambda x: x[0])
            top_pairs = pairs[:10]
            ms.write(f"{lab}\t{n}\t")
            ms.write(';'.join([f"{a:.6g},{s:.6g},{x},{y}" for a,s,x,y in top_pairs]))
            ms.write('\n')
            # heatmap
            try:
                fig, ax = plt.subplots(figsize=(6, max(3, len(numeric_keys)*0.22)))
                im = ax.imshow(C, cmap='coolwarm', vmin=-1, vmax=1)
                ax.set_xticks(np.arange(len(numeric_keys)))
                ax.set_yticks(np.arange(len(numeric_keys)))
                ax.set_xticklabels(numeric_keys, rotation=45, ha='right', fontsize=7)
                ax.set_yticklabels(numeric_keys, fontsize=7)
                fig.colorbar(im, ax=ax, fraction=0.04, pad=0.03)
                plt.tight_layout()
                out_img = os.path.join(outdir, f"{safe}_correlation_heatmap.png")
                plt.savefig(out_img)
                plt.close()
            except Exception:
                pass
    print('Wrote grouped correlation summaries to', outdir)
    return True


def compute_grouped_correlations_by_workload(csv_path, min_rows=3):
    """Compute correlations grouped by workload (`container/test`).

    Outputs under `results/grouped_workload/`:
      - `<sanitized_workload>_correlation_matrix.csv`
      - `<sanitized_workload>_correlation_heatmap.png`
    Also writes a master summary `correlation_summary_by_workload.txt`.
    """
    try:
        import numpy as np
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except Exception:
        print('numpy/matplotlib required for grouped workload correlations; skipping')
        return False

    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    if not rows:
        print('no rows for grouped workload correlations')
        return False

    def workload_key(r):
        cont = r.get('container') or 'NA'
        test = r.get('test') or ''
        if test:
            return f"{cont}/{test}"
        return cont

    groups = defaultdict(list)
    for r in rows:
        groups[workload_key(r)].append(r)

    outdir = os.path.join(RESULTS, 'grouped_workload')
    os.makedirs(outdir, exist_ok=True)
    master_summary = os.path.join(outdir, 'correlation_summary_by_workload.txt')
    with open(master_summary, 'w', encoding='utf-8') as ms:
        ms.write('Workload\tN\tTop correlations (abs,signed,key1,key2)\n')
        for wl, items in sorted(groups.items(), key=lambda x: -len(x[1])):
            n = len(items)
            if n < min_rows:
                ms.write(f"{wl}\t{n}\tSKIPPED (n<{min_rows})\n")
                continue
            numeric_keys = ['total_pages', 'page_size', 'payload', 'rps', 'framerate', 'avg_entropy', 'avg_zero_ratio', 'zero_pages', 'zero_pages_ratio', 'unique_hashes', 'duplicate_pages']
            mat = []
            for r in items:
                # ensure zero_pages_ratio
                try:
                    zp = float(r.get('zero_pages', 'nan'))
                except Exception:
                    zp = float('nan')
                try:
                    tp = float(r.get('total_pages', 'nan'))
                except Exception:
                    tp = float('nan')
                zr = float('nan')
                if not np.isnan(zp) and not np.isnan(tp) and tp > 0:
                    zr = zp / tp
                r['zero_pages_ratio'] = zr
                rowvals = []
                for k in numeric_keys:
                    v = r.get(k, '')
                    try:
                        rowvals.append(float(v))
                    except Exception:
                        rowvals.append(np.nan)
                mat.append(rowvals)
            A = np.array(mat, dtype=float)
            good = ~np.all(np.isnan(A), axis=1)
            A = A[good]
            if A.shape[0] < min_rows:
                ms.write(f"{wl}\t{n}\tSKIPPED_AFTER_NA_FILTER (effective<{min_rows})\n")
                continue
            col_means = np.nanmean(A, axis=0)
            inds = np.where(np.isnan(A))
            A[inds] = np.take(col_means, inds[1])
            try:
                C = np.corrcoef(A, rowvar=False)
            except Exception:
                ms.write(f"{wl}\t{n}\tFAILED_CORR\n")
                continue
            safe = re.sub(r'[^0-9a-zA-Z_-]', '_', wl)[:180]
            out_csv = os.path.join(outdir, f"{safe}_correlation_matrix.csv")
            with open(out_csv, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow([''] + numeric_keys)
                for i,k in enumerate(numeric_keys):
                    w.writerow([k] + [f"{float(v):.6g}" for v in C[i]])
            pairs = []
            m = len(numeric_keys)
            for i in range(m):
                for j in range(i+1, m):
                    pairs.append((abs(C[i,j]), C[i,j], numeric_keys[i], numeric_keys[j]))
            pairs.sort(reverse=True, key=lambda x: x[0])
            top_pairs = pairs[:10]
            ms.write(f"{wl}\t{n}\t")
            ms.write(';'.join([f"{a:.6g},{s:.6g},{x},{y}" for a,s,x,y in top_pairs]))
            ms.write('\n')
            try:
                fig, ax = plt.subplots(figsize=(6, max(3, len(numeric_keys)*0.22)))
                im = ax.imshow(C, cmap='coolwarm', vmin=-1, vmax=1)
                ax.set_xticks(np.arange(len(numeric_keys)))
                ax.set_yticks(np.arange(len(numeric_keys)))
                ax.set_xticklabels(numeric_keys, rotation=45, ha='right', fontsize=7)
                ax.set_yticklabels(numeric_keys, fontsize=7)
                fig.colorbar(im, ax=ax, fraction=0.04, pad=0.03)
                plt.tight_layout()
                out_img = os.path.join(outdir, f"{safe}_correlation_heatmap.png")
                plt.savefig(out_img)
                plt.close()
            except Exception:
                pass
    print('Wrote grouped-by-workload correlation summaries to', outdir)
    return True


def print_combo_labels(csv_path, topn=20):
    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    labels = []
    for r in rows:
        cont=r.get('container') or 'NA'
        test=r.get('test') or 'NA'
        pm=r.get('payload_mode') or 'NA'
        pat=r.get('pattern') or 'NA'
        sd=r.get('size_distribution') or 'NA'
        p=r.get('payload')
        try:
            pstr=str(int(float(p)))
        except Exception:
            pstr=str(p) if p and str(p).strip() else 'NA'
        rps=r.get('rps')
        try:
            rpss=str(int(float(rps)))
        except Exception:
            rpss='NA'
        dist = sd if sd and sd != 'NA' else pat or 'NA'
        lab=f"{cont}/{test}|{pm}|{dist}|p{pstr}|r{rpss}"
        labels.append(lab)
    cnt = Counter(labels)
    print('\nTop %d combo labels:' % topn)
    for i,(k,v) in enumerate(cnt.most_common(topn),1):
        print(f"{i:2d}. {k}  (n={v})")


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--force', '-f', action='store_true', help='Force re-aggregation of dump analyses (overwrite full_run_summary.csv)')
    args = p.parse_args()

    csv_path = os.path.join(RESULTS, 'full_run_summary.csv')
    if args.force or not os.path.exists(csv_path):
        print('Aggregating CRIU dump analyses...')
        csv_path = aggregate_results()
    else:
        # If the existing CSV does not reflect the dumps on disk (e.g. a new
        # workload was added after the CSV was created), re-run aggregation.
        try:
            # containers in dump dirs
            dump_containers = set()
            for name in os.listdir(RESULTS):
                if not name.endswith('_dump'):
                    continue
                d = os.path.join(RESULTS, name)
                if not os.path.isdir(d):
                    continue
                meta = parse_dirname(d)
                dump_containers.add(meta.get('container'))

            # containers in existing CSV
            csv_containers = set()
            with open(csv_path, newline='', encoding='utf-8') as f:
                r = csv.DictReader(f)
                for row in r:
                    c = row.get('container') or row.get('cont') or ''
                    if c:
                        csv_containers.add(c)

            if not dump_containers.issubset(csv_containers):
                print('Detected new dump directories (workloads) not present in existing CSV; re-aggregating...')
                csv_path = aggregate_results()
            else:
                print('Using existing CSV (no new dumps detected).')
        except Exception:
            # fallback: re-aggregate if any unexpected error occurs while
            # inspecting the CSV or dump dirs
            print('Failed to validate existing CSV, re-aggregating...')
            csv_path = aggregate_results()
    print('Filling size_distribution when reasonable...')
    fill_size_distribution(csv_path)
    print('Computing byte-level summary and correlations...')
    compute_byte_level_summary(csv_path)
    print('Computing grouped correlations by workload/combo...')
    compute_grouped_correlations(csv_path, min_rows=3)
    print('Computing grouped correlations by workload (container/test)...')
    compute_grouped_correlations_by_workload(csv_path, min_rows=3)
    print('Generating combo plots...')
    make_plots(csv_path)
    # comparison bars removed per user request
    print('Running simple model...')
    analyze_models(csv_path)
    print_combo_labels(csv_path)
    print('\nConsolidated analysis finished.')


if __name__ == '__main__':
    main()
