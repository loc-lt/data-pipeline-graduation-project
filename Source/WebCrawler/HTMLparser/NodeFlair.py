import pandas as pd
def crawl():
    transform_df = pd.read_json('./Generatueurl/jobs.json')

    base_url = 'https://nodeflair.com'
    # transform job posting link (job_path)
    transform_df['job_path'] = base_url + transform_df['job_path'] 
    def stacks_to_list(all_stacks): 
        return ', '.join([list(stack.values())[0] for stack in all_stacks])
    
    transform_df['tech_stacks'] = transform_df['tech_stacks'].apply(stacks_to_list)

    return transform_df[["job_path","position","title","salary_min","salary_max","currency","tech_stacks"]]