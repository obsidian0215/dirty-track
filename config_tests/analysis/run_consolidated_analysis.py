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
"""
import os
import sys
import json
import csv
import re
import statistics
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
        unique_pages = tp - dp if not np.isnan(tp) and not np.isnan(dp) else float('nan')
        stats.setdefault(lab, []).append({'total': tp, 'zero': zp, 'dup': dp, 'const': cp, 'unique': unique_pages})

    combo_list = []
    for lab, items in stats.items():
        totals = np.array([it['total'] for it in items], dtype=float)
        zeros = np.array([it['zero'] for it in items], dtype=float)
        dups = np.array([it['dup'] for it in items], dtype=float)
        consts = np.array([it['const'] for it in items], dtype=float)
        def mean_safe(a):
            if np.all(np.isnan(a)):
                return float('nan')
            return np.nanmean(a)
        m_total = mean_safe(totals)
        m_zero = mean_safe(zeros)
        m_dup = mean_safe(dups)
        m_const = mean_safe(consts)
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
        combo_list.append({'lab': lab, 'total': m_total, 'prop_zero': prop_zero, 'prop_dup': prop_dup, 'prop_const': prop_const, 'prop_uniq_non': prop_uniq_non, 'count': len(items)})

    combo_list.sort(key=lambda x: (0 if np.isnan(x['total']) else x['total']), reverse=True)
    TOP = 40
    top = combo_list[:TOP]
    labels = [c['lab'] for c in top]
    totals = [0 if np.isnan(c['total']) else c['total'] for c in top]
    zeros = [c['prop_zero'] for c in top]
    dups = [c['prop_dup'] for c in top]
    consts = [c['prop_const'] for c in top]
    uniqs = [c['prop_uniq_non'] for c in top]

    y = np.arange(len(labels))
    workloads = [l.split('|', 1)[0] for l in labels]
    unique_w = sorted(set(workloads))
    cmap = plt.get_cmap('tab20')
    wcolor_map = {w: cmap(i % 20) for i, w in enumerate(unique_w)}

    fig, ax = plt.subplots(figsize=(10, max(6, len(labels)*0.18)))
    seg_colors = {'zero': '#6baed6', 'dup': '#fd8d3c', 'uniq': '#74c476', 'const': '#9e9ac8'}
    for i in range(len(labels)):
        wc = wcolor_map.get(workloads[i], (0.5,0.5,0.5))
        left = 0.0
        ax.barh(y[i], zeros[i], left=left, color=seg_colors['zero'], edgecolor=wc, linewidth=0.8)
        left += zeros[i]
        ax.barh(y[i], dups[i], left=left, color=seg_colors['dup'], edgecolor=wc, linewidth=0.8)
        left += dups[i]
        ax.barh(y[i], uniqs[i], left=left, color=seg_colors['uniq'], edgecolor=wc, linewidth=0.8)
        left += uniqs[i]
        ax.barh(y[i], consts[i], left=left, color=seg_colors['const'], edgecolor=wc, linewidth=0.8)
        ax.barh(y[i], 0.01, left=-0.02, color=wc, edgecolor=wc, height=0.6)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel('fraction of total_pages')
    ax.set_title('Page-type proportions by combo (top %d combos by mean total_pages)' % TOP)
    seg_handles = [plt.Rectangle((0,0),1,1,color=seg_colors[k]) for k in ('zero','dup','uniq','const')]
    seg_labels = ['zero_pages','duplicate_pages','unique_nonzero_nonconst','constant_pages']
    ax.legend(seg_handles, seg_labels, loc='lower right')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'page_type_proportions_by_combo_top%d.png' % TOP))
    plt.close()

    fig, ax = plt.subplots(figsize=(10, max(4, len(labels)*0.12)))
    ax.barh(y, totals, color=[plt.get_cmap('tab20')(i % 20) for i in range(len(labels))])
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=8)
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
    csv_path = os.path.join(RESULTS, 'full_run_summary.csv')
    if not os.path.exists(csv_path):
        print('Aggregating CRIU dump analyses...')
        csv_path = aggregate_results()
    print('Filling size_distribution when reasonable...')
    fill_size_distribution(csv_path)
    print('Generating combo plots...')
    make_plots(csv_path)
    print('Running simple model...')
    analyze_models(csv_path)
    print_combo_labels(csv_path)
    print('\nConsolidated analysis finished.')


if __name__ == '__main__':
    main()
