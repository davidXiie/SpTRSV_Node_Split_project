import os
import json
import collections
import glob
import csv

class HeteroScheduler:
    """
    Hetero Scheduler (Mode A Exact Logic)
    - LC-Driven: Edges are released at the start of next LC.
    - Single-Edge Promotion: Matches MEC's 1-op-per-cycle assumption.
    - Structural Hazards: Enforced during Issue.
    """
    def __init__(self, dag_data, mec_map, pe_limit=10, nfu_limit=1):
        self.dag_data = dag_data
        self.mec_map = mec_map
        self.pe_limit = pe_limit
        self.nfu_limit = nfu_limit
        
        # --- 1. Graph & State Init ---
        self.adj_list = collections.defaultdict(list)
        self.node_info = {}
        self.node_remaining_dependency_count = {} 
        self.fusion_remaining_parents = {}        
        
        self._build_graph()
        
        # --- 2. Runtime State ---
        self.current_lc = 0
        self.pc = 0
        self.finished_nodes = set()
        self.nodes_finished_last_lc = []
        
        # Queues
        self.optional_edge_queue = []
        self.mandatory_queue = []
        
        # Events
        self.pe_events = [] 
        self.nfu_events = []
        
        # Resources
        self.free_pes = self.pe_limit
        self.nfu_busy_timer = 0
        
        # Logs
        self.trace_log = []
        self.detailed_log = []
        self.lc_snapshot_log = []
        self.stats = {'total_cycles': 0, 'pe_active_cycles': 0, 'nfu_active_cycles': 0}

    def _build_graph(self):
        for item in self.dag_data:
            nid = item['id']
            parents = item['parents']
            ntype = item['type']
            mec_val = self.mec_map.get(str(nid), 99999)
            
            self.node_info[nid] = {
                'type': ntype,
                'mec': mec_val,
                'parents': parents
            }
            
            for p in parents:
                self.adj_list[str(p)].append(nid)
                
            if ntype == 'FUSION':
                self.fusion_remaining_parents[nid] = len(parents)
            else:
                self.node_remaining_dependency_count[nid] = len(parents)

    def _calc_slack(self, target_id):
        mec = self.node_info[target_id]['mec']
        rem = self.node_remaining_dependency_count.get(target_id, 0)
        return mec - self.current_lc - rem

    def run(self):
        total_nodes = len(self.dag_data)
        max_lc_limit = 5000 
        
        while len(self.finished_nodes) < total_nodes:
            if self.current_lc > max_lc_limit:
                print(f"timeout {self.current_lc}")
                break
                
            # ============================
            # 1. LC 推进
            # ============================
            self.current_lc += 1
            nodes_finished_this_lc = []
            
            # [Step A] 释放上一个 LC 完成节点产生的边任务 (Batch Release)
            for src_id in self.nodes_finished_last_lc:
                children = self.adj_list[str(src_id)]
                for child_id in children:
                    child_type = self.node_info[child_id]['type']
                    if child_type == 'FUSION':
                        # Fusion 依赖计数减少在 completion 时已做，这里主要处理 MEC 触发
                        pass
                    else:
                        child_mec = self.node_info[child_id]['mec']
                        task = {
                            'type': 'EDGE',
                            'src': src_id,
                            'id': child_id,
                            'mec': child_mec,
                            'slack': 0 # placeholder
                        }
                        self.optional_edge_queue.append(task)
            
            self.nodes_finished_last_lc = []

            # [Step B] 检查新的 Mandatory 任务 (Update / Fusion Deadline Reached)
            for nid, info in self.node_info.items():
                if nid in self.finished_nodes: continue
                
                ntype = info['type']
                mec = info['mec']
                
                if mec <= self.current_lc:
                    is_ready = False
                    if ntype == 'FUSION':
                        if self.fusion_remaining_parents.get(nid, 0) == 0: is_ready = True
                    elif ntype == 'NORMAL':
                        if self.node_remaining_dependency_count.get(nid, 0) == 0: is_ready = True
                    
                    if is_ready:
                        in_q = any(t['id'] == nid and t['type'] in ['UPDATE', 'FUSION'] for t in self.mandatory_queue)
                        in_run = any(e['target_id'] == nid and e['type'] in ['UPDATE', 'FUSION'] for e in self.pe_events + self.nfu_events)
                        
                        if not in_q and not in_run:
                            op_type = 'FUSION' if ntype == 'FUSION' else 'UPDATE'
                            self.mandatory_queue.append({
                                'type': op_type,
                                'id': nid,
                                'slack': -999,
                                'mec': mec
                            })

            # [Step C] Slack 抢占 (Single-Edge Promotion Logic)
            # 1. 对 Optional Queue 按 Target 分组
            tasks_by_target = collections.defaultdict(list)
            for task in self.optional_edge_queue:
                tasks_by_target[task['id']].append(task)
            
            # 清空原队列，准备重构
            self.optional_edge_queue = []
            
            # 2. 遍历每个 Target，计算 Slack，决定是否提升
            for tid, tasks in tasks_by_target.items():
                slack = self._calc_slack(tid)
                
                # 更新所有任务的 Slack
                for t in tasks: t['slack'] = slack
                
                # 如果 Slack <= 0，且还有任务：
                # ！！！关键点：只提升第一个任务进入 Mandatory ！！！
                # 剩余的保留在 Optional
                if slack <= 0 and tasks:
                    promoted_task = tasks[0]
                    self.mandatory_queue.append(promoted_task)
                    
                    # 剩下的放回 Optional
                    for t in tasks[1:]:
                        self.optional_edge_queue.append(t)
                else:
                    # Slack > 0，全部保留在 Optional
                    for t in tasks:
                        self.optional_edge_queue.append(t)

            # [快照]
            self._record_lc_snapshot()

            # ============================
            # 2. PC 循环
            # ============================
            first_pass = True
            
            while first_pass or self.mandatory_queue:
                first_pass = False
                self.pc += 1
                
                # --- Phase 2.1: Retire ---
                active_pe_events = []
                for event in self.pe_events:
                    if event['finish_time'] <= self.pc:
                        self._handle_completion(event)
                    else:
                        active_pe_events.append(event)
                self.pe_events = active_pe_events
                self.free_pes = self.pe_limit - len(self.pe_events)

                active_nfu_events = []
                for event in self.nfu_events:
                    if event['finish_time'] <= self.pc:
                        self._handle_completion(event)
                    else:
                        active_nfu_events.append(event)
                self.nfu_events = active_nfu_events
                
                if self.nfu_busy_timer > 0: self.nfu_busy_timer -= 1

                # --- Phase 2.2: Issue ---
                dispatched_log = []
                locked_targets = set()
                for e in self.pe_events: locked_targets.add(e['target_id'])
                
                # A. Mandatory
                self.mandatory_queue.sort(key=lambda x: x['slack'])
                next_mandatory = []
                for task in self.mandatory_queue:
                    # 尝试发射，成功则记录进 LC 完成列表 (仅 Update/Fusion 算节点完成)
                    is_node_complete_type = (task['type'] in ['UPDATE', 'FUSION'])
                    target_list = nodes_finished_this_lc if is_node_complete_type else None
                    
                    success = self._try_dispatch(task, locked_targets, dispatched_log, target_list)
                    if not success:
                        next_mandatory.append(task)
                self.mandatory_queue = next_mandatory
                
                # B. Optional (如果有空余 PE)
                self.optional_edge_queue.sort(key=lambda x: (x['slack'], x['mec']))
                next_optional = []
                for task in self.optional_edge_queue:
                    if self.free_pes > 0:
                        success = self._try_dispatch(task, locked_targets, dispatched_log, None)
                        if not success:
                            next_optional.append(task)
                    else:
                        next_optional.append(task)
                self.optional_edge_queue = next_optional

                # --- Phase 2.3: Log ---
                self._record_log(dispatched_log)
                
                if not self.mandatory_queue:
                    break
            
            # End of PC loop
            self.nodes_finished_last_lc = nodes_finished_this_lc

        return self.stats, self.trace_log, self.detailed_log, self.lc_snapshot_log

    def _try_dispatch(self, task, locked_targets, log_list, finished_list_lc):
        tid = task['id']
        ttype = task['type']
        
        # 结构冒险检查
        if tid in locked_targets: return False
        
        if ttype == 'FUSION':
            if self.nfu_busy_timer == 0:
                self.nfu_busy_timer = 1
                finish_time = self.pc + 2
                self.nfu_events.append({
                    'type': 'FUSION', 'target_id': tid,
                    'finish_time': finish_time, 'start_pc': self.pc,
                    'ops_str': [f"FusionCalc({tid})", f"FusionWB({tid})"],
                    'mec': task['mec']
                })
                log_list.append(f"NFU:{tid}")
                # Fusion 节点只要派发了就算在本 LC 逻辑上被处理了
                if finished_list_lc is not None: finished_list_lc.append(tid)
                return True
            return False
                
        elif ttype in ['EDGE', 'UPDATE']:
            if self.free_pes > 0:
                self.free_pes -= 1
                finish_time = self.pc + 1
                op_str = f"Edge({task['src']}->{tid})" if ttype == 'EDGE' else f"Update({tid})"
                
                self.pe_events.append({
                    'type': ttype, 'target_id': tid,
                    'finish_time': finish_time, 'start_pc': self.pc,
                    'ops_str': [op_str],
                    'mec': task['mec']
                })
                locked_targets.add(tid)
                log_list.append(op_str)
                # Update 算节点完成
                if ttype == 'UPDATE' and finished_list_lc is not None:
                    finished_list_lc.append(tid)
                return True
            return False
        
        return False

    def _handle_completion(self, event):
        tid = event['target_id']
        etype = event['type']
        
        if etype == 'EDGE':
            self.node_remaining_dependency_count[tid] -= 1
            if self.node_info[tid]['type'] == 'PARTIAL':
                if self.node_remaining_dependency_count[tid] == 0:
                    self._mark_node_finished_physically(tid)
            # Normal 节点的 Edge 完成不直接触发 Update，由下一轮 LC 检查 Deadline 触发
            
        elif etype in ['UPDATE', 'FUSION']:
            self._mark_node_finished_physically(tid)

    def _mark_node_finished_physically(self, nid):
        self.finished_nodes.add(nid)
        # 实时逻辑仅用于 Fusion 内部计数器，边任务释放已移至 LC 循环开头
        children = self.adj_list[str(nid)]
        for child_id in children:
            child_type = self.node_info[child_id]['type']
            if child_type == 'FUSION':
                self.fusion_remaining_parents[child_id] -= 1

    def _record_lc_snapshot(self):
        mand_str_list = []
        for t in self.mandatory_queue:
            if t['type'] == 'EDGE':
                mand_str_list.append(f"E({t['src']}->{t['id']})")
            else:
                prefix = 'U' if t['type'] == 'UPDATE' else 'F'
                mand_str_list.append(f"{prefix}({t['id']})")
        
        opt_str_list = []
        for t in self.optional_edge_queue:
            opt_str_list.append(f"E({t['src']}->{t['id']},S:{t['slack']})")
            
        snapshot = {
            'lc': self.current_lc,
            'pc_start': self.pc + 1,
            'mandatory': mand_str_list,
            'optional': opt_str_list
        }
        self.lc_snapshot_log.append(snapshot)

    def _record_log(self, dispatched):
        pe_util = self.pe_limit - self.free_pes
        nfu_util = 1 if self.nfu_busy_timer > 0 else 0
        self.stats['pe_active_cycles'] += pe_util
        self.stats['nfu_active_cycles'] += nfu_util
        self.stats['total_cycles'] = self.pc
        
        if dispatched:
            self.trace_log.append({
                'pc': self.pc, 'pe_busy': pe_util, 'nfu_busy': nfu_util, 'ops': " ".join(dispatched)
            })
            
        running_ops = []
        for e in self.pe_events: running_ops.append(f"PE: {e['ops_str'][0]:<15} [LC:{e['mec']}]")
        for e in self.nfu_events: 
            idx = self.pc - e['start_pc']
            op = e['ops_str'][idx] if idx < len(e['ops_str']) else "Wait"
            running_ops.append(f"NFU: {op:<15} [LC:{e['mec']}]")
            
        snapshot = {
            'pc': self.pc,
            'lc': self.current_lc,
            'dispatched': dispatched,
            'pe_active': pe_util, 'nfu_active': nfu_util,
            'mand_q': [f"{t['id']}({t['type']})" for t in self.mandatory_queue],
            'opt_q': [f"E({t['src']}->{t['id']})" for t in self.optional_edge_queue[:10]],
            'running': running_ops
        }
        self.detailed_log.append(snapshot)

def batch_run_scheduler_hetero(dag_dir, mec_dir, output_dir, pe_count=10, nfu_count=1):
    os.makedirs(output_dir, exist_ok=True)
    dag_files = glob.glob(os.path.join(dag_dir, "*_split.json"))
    summary_data = []
    print(f"Starting LC-Driven Hetero Scheduler (PE={pe_count}, NFU={nfu_count})...")
    
    for dag_path in dag_files:
        fname = os.path.basename(dag_path).replace("_split.json", "")
        mec_path = os.path.join(mec_dir, f"{fname}_mec_resource.json")
        if not os.path.exists(mec_path): continue
        
        print(f"Scheduling: {fname}")
        with open(dag_path, 'r') as f: dag_data = json.load(f)
        with open(mec_path, 'r') as f: mec_map = json.load(f)
        
        scheduler = HeteroScheduler(dag_data, mec_map, pe_limit=pe_count, nfu_limit=nfu_count)
        stats, trace, detailed, lc_logs = scheduler.run()
        
        debug_path = os.path.join(output_dir, f"{fname}_hetero_debug_full.txt")
        with open(debug_path, 'w') as f:
            f.write(f"FULL DEBUG (LC-Driven): {fname}\n")
            f.write("Format: [LC xxxx][PC xxxx] -> Ops\n")
            f.write("=" * 80 + "\n")
            for d in detailed:
                f.write(f"[LC {d['lc']:04d}][PC {d['pc']:04d}]\n")
                f.write(f"  STATE: PE:{d['pe_active']}/{pe_count} NFU:{d['nfu_active']}/{nfu_count}\n")
                if d['dispatched']: f.write(f"  ISSUE: {', '.join(d['dispatched'])}\n")
                if d['running']:
                    for r in d['running']: f.write(f"    -> {r}\n")
                if d['mand_q']: f.write(f"  MAND_Q: {', '.join(d['mand_q'])}\n")
                if d['opt_q']: f.write(f"  OPT_Q (Top10): {', '.join(d['opt_q'])}\n")
                f.write("-" * 50 + "\n")

        lc_debug_path = os.path.join(output_dir, f"{fname}_hetero_lc_debug.txt")
        with open(lc_debug_path, 'w') as f:
            f.write(f"LC-Level Task Analysis: {fname}\n")
            f.write("=" * 80 + "\n")
            for log in lc_logs:
                f.write(f"[LC {log['lc']:04d}] (Starts ~ PC {log['pc_start']:04d})\n")
                mand_count = len(log['mandatory'])
                f.write(f"  Mandatory Queue ({mand_count} tasks):\n")
                if mand_count > 0:
                    chunks = [log['mandatory'][i:i+5] for i in range(0, mand_count, 5)]
                    for chunk in chunks: f.write(f"    {', '.join(chunk)}\n")
                else: f.write("    (Empty)\n")
                
                opt_count = len(log['optional'])
                f.write(f"  Optional Queue ({opt_count} tasks):\n")
                if opt_count > 0:
                    limit = 20
                    to_show = log['optional'][:limit]
                    chunks = [to_show[i:i+5] for i in range(0, len(to_show), 5)]
                    for chunk in chunks: f.write(f"    {', '.join(chunk)}\n")
                    if opt_count > limit: f.write(f"    ... and {opt_count - limit} more\n")
                else: f.write("    (Empty)\n")
                f.write("-" * 50 + "\n")
                
        summary_data.append({'Matrix': fname, 'Cycles': stats['total_cycles']})
        
    csv_path = os.path.join(output_dir, "hetero_summary.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Matrix', 'Cycles'])
        writer.writeheader()
        writer.writerows(summary_data)

if __name__ == "__main__":
    BASE_DIR = os.getcwd()
    DAG_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "input_data_split")
    MEC_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "output_data", "mec_resource")
    OUTPUT_DIR = os.path.join(BASE_DIR, "SpTRSV_Node_Split_project", "output_data", "scheduler_hetero")
    batch_run_scheduler_hetero(DAG_DIR, MEC_DIR, OUTPUT_DIR, pe_count=10, nfu_count=1)