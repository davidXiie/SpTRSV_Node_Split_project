import os
import json
import random
import matplotlib.pyplot as plt

class SyntheticDAGGenerator:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_and_save(self, dim, sparsity, name_suffix="", visualize=True):
        """
        生成合成 L 矩阵并保存为 DAG JSON (Standardized Format)
        """
        print(f"Generating Matrix: Dim={dim}, Target Sparsity={sparsity:.2%}")
        
        adjacency = {i: [] for i in range(dim)}
        levels = {i: 0 for i in range(dim)}
        viz_rows = []
        viz_cols = []
        
        total_possible_edges = (dim * (dim - 1)) / 2
        actual_edges = 0
        
        # 生成随机下三角依赖
        for i in range(dim):
            viz_rows.append(i)
            viz_cols.append(i)
            
            max_parent_level = -1
            current_row_parents = []
            
            for j in range(i):
                if random.random() < sparsity:
                    current_row_parents.append(j)
                    actual_edges += 1
                    viz_rows.append(i)
                    viz_cols.append(j)
                    if levels[j] > max_parent_level:
                        max_parent_level = levels[j]
            
            adjacency[i] = current_row_parents
            levels[i] = max_parent_level + 1
            
        # [修改点] 统一输出格式: id, parents, is_super
        dag_list = []
        for i in range(dim):
            dag_list.append({
                "id": i,                        # 原 row_index
                "parents": adjacency[i],        # 原 dependency_nodes
                "level": levels[i],
                "is_super": False,              # 统一字段
                "cost": len(adjacency[i]) + 1   # 预估 Cost (可选)
            })
            
        base_name = f"synthetic_{dim}_{int(sparsity*100)}sp{name_suffix}"
        json_path = os.path.join(self.output_dir, f"{base_name}_dag.json")
        
        with open(json_path, 'w') as f:
            json.dump(dag_list, f, indent=4)
            
        if visualize:
            self._save_visualization(base_name, dim, viz_rows, viz_cols)

        print(f"  -> Saved JSON: {json_path}")
        return json_path

    def _save_visualization(self, base_name, dim, rows, cols):
        plt.figure(figsize=(6, 6))
        point_size = 200.0 / dim if dim > 0 else 2
        if point_size < 0.5: point_size = 0.5
        
        plt.scatter(cols, rows, c='blue', s=point_size, marker='s', edgecolors='none')
        plt.xlim(-1, dim)
        plt.ylim(-1, dim)
        plt.gca().invert_yaxis()
        plt.title(f"Sparsity Pattern: {base_name}")
        plt.grid(False)
        
        img_path = os.path.join(self.output_dir, f"{base_name}_spy.png")
        plt.savefig(img_path, dpi=150)
        plt.close()

if __name__ == "__main__":
    OUTPUT_DIR = os.path.join(os.getcwd(), "SpTRSV_Node_Split_project", "input_data")
    generator = SyntheticDAGGenerator(OUTPUT_DIR)
    
    # Batch Generation
    DIMENSION = 100 
    for sp in [0.05, 0.10, 0.20]:
        generator.generate_and_save(dim=DIMENSION, sparsity=sp)