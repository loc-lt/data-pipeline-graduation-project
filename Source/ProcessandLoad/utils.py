from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from datetime import date
import utils as u
from bisect import bisect_left
import pandas as pd
def geturlpath(website):
    if website == 'Coursera':
        return 'hdfs:////Courses/Urlcrawled/Coursera'
    elif website == 'edX':
        return 'hdfs:////Courses/Urlcrawled/edX'
    elif website == 'Udemy':
        return 'hdfs:////Courses/Urlcrawled/Udemy'
    elif website == 'Dice':
        return 'hdfs:////JobPostings/Urlcrawled/Dice'
    elif website == 'NodeFlair':
        return 'hdfs:////JobPostings/Urlcrawled/NodeFlair'
    
def getdatapath(website):
    if website == 'Coursera':
        return 'hdfs:////Courses/Coursera'
    elif website == 'edX':
        return 'hdfs:////Courses/edX'
    elif website == 'Udemy':
        return 'hdfs:////Courses/Udemy'
    elif website == 'Dice':
        return 'hdfs:////JobPostings/Dice'
    elif website == 'NodeFlair':
        return 'hdfs:////JobPostings/NodeFlair'
    
# Tạo DataFrame mẫu
#df = spark.createDataFrame(data, columns)
def etract_skill(result_df,colname, mapping_dict):
# Hàm ánh xạ tùy chỉnh
    def map_skill(skill, mapping_dict):
        mapped_skills = []
        for key in mapping_dict.keys():
            if key in skill:
                mapped_skills.append(mapping_dict[key])
        if len(mapped_skills) == 0:
            return None
        else:
            return ', '.join(mapped_skills)

    # Tạo từ điển cho cột skill

    # Đăng ký hàm ánh xạ tùy chỉnh với PySpark và truyền biến mapping_dict
    map_skill_udf = f.udf(lambda skill: map_skill(skill, mapping_dict), StringType())

    # Áp dụng hàm ánh xạ tùy chỉnh vào cột "skill" của DataFrame
    result_df = result_df.withColumn(colname, map_skill_udf(result_df['skill']))
    return result_df


def insert_cassandra_dw_course(session,pandas_df,key_space):
    insert_query = session.prepare(f"""
    INSERT INTO {key_space}.course
    ("name", "link", "rating", "enroll", "instructor", "time", "level", "fee", "program", "subtitle", "subject", "organization", "programming_language", "tool", "framework","platform")
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

    for index, row in pandas_df.iterrows():
        session.execute(insert_query, (
            row['name'],row['link'],row['rating'], row['enroll'],row['instructor'],
            row['time'],row['level'],row['fee'],row['program'],row['subtitle'],
            row['subject'],row['organization'],row['programming_language'],row['tool'],
            row['framework'],row['platform']
        ))

#####subject_count_enroll
def update_subject_enroll(session,pandas_df,key_space):
    subject_count_enroll = pandas_df.groupby(pandas_df['subject']).aggregate(
        {'link': 'count', 'enroll': 'sum'}) 

    subject_count_enroll["enroll"] = subject_count_enroll["enroll"].astype(int)

    def execute_update_subject_enroll(session,subject, total_enroll, total_course,key_space):
    
        # Update existing row if the subject already exists
        update_query = f"UPDATE {key_space}.subject_course_enroll SET total_enroll = total_enroll + {total_enroll}, total_course = total_course + {total_course} WHERE subject = '{subject}'"
        session.execute(update_query)


    for index, row in subject_count_enroll.iterrows():
        subject = index
        link = row['link']
        enroll = row['enroll']
        execute_update_subject_enroll(session,subject,enroll,link,key_space)

###subject_level_time_fee
def update_subject_level_time_fee(session,pandas_df,key_space):

    subject_level_time_fee = pandas_df.groupby(['subject','level']).aggregate(
    {'link': 'count', 'time': 'mean','fee':'mean'}) 
    subject_level_time_fee["link"] = subject_level_time_fee["link"].astype(int)

    def execute_update_subject_level_time_fee(session,subject, level, time, fee, courses,key_space):
        get_cur_time = f"SELECT time, fee ,total_course FROM {key_space}.subject_level_time_fee WHERE subject = '{subject}' AND level = '{level}'"
        result = session.execute(get_cur_time).one()
        curtime = result.time if result else 0.0
        curfee = result.fee if result else 0.0
        curtotalcourses = result.total_course if result else 0
        total = curtotalcourses + int(courses)
        averagetime = (curtime*curtotalcourses + time*courses)/total
        averagefee = (curfee*curtotalcourses + fee*courses)/total
        
        # Update existing
        update_query = f"UPDATE {key_space}.subject_level_time_fee SET time = {averagetime}, total_course = {total}, fee = {averagefee} WHERE subject = '{subject}' AND level = '{level}'"
        session.execute(update_query)

    for index, row in subject_level_time_fee.iterrows():
        subject = index[0]
        
        level = index[1]
        if level =="":
            level = "None"
        link = row['link']
        time = row['time']
        fee = row['fee']
        execute_update_subject_level_time_fee(session,subject,level,time,fee,link,key_space)

###subject_language_course
###  subject_language_course
def update_subject_language_course(session,pandas_df,key_space):
    def subject_language_course(df):
        # Chuyển cột 'programing_language' sang kiểu dữ liệu chuỗi (str)
        df['programming_language'] = df['programming_language'].astype(str)

        # Phân tách chuỗi ngôn ngữ lập trình và sử dụng explode để tách thành từng dòng riêng biệt
        new_df = df.assign(programing_language=df['programming_language'].str.split(',')).explode('programming_language')

        # Nhóm theo chủ đề (subject) và ngôn ngữ lập trình, sau đó đếm số lần xuất hiện
        return new_df.groupby(['subject', 'programming_language']).size().reset_index(name='count')

    def execute_update_subject_language_course(session,subject, language, courses, keyspace):
        update_query = f"UPDATE {keyspace}.subject_language_course SET total_course = total_course + {courses} WHERE subject = '{subject}' AND programming_language = '{language}'"
        session.execute(update_query)
    # Hiển thị DataFrame mới
    df = subject_language_course(pandas_df)
    for index, row in df.iterrows():
        subject = row['subject']
        programming_language = row['programming_language']
        if programming_language=="":
            programming_language="None"
        count = row['count']
        execute_update_subject_language_course(session,subject, programming_language, count, key_space)


def update_subject_framework_course(session,pandas_df,key_space):
    
    def subject_framework_course(df):
        df['framework'] = df['framework'].astype(str)

        # Phân tách chuỗi framework và sử dụng explode để tách thành từng dòng riêng biệt
        new_df = df.assign(framework=df['framework'].str.split(',')).explode('framework')

        # Nhóm theo chủ đề (subject) và framework, sau đó đếm số lần xuất hiện
        return new_df.groupby(['subject', 'framework']).size().reset_index(name='count')

    def execute_update_subject_framework_course(session,subject, framework, courses, keyspace):
        update_query = f"UPDATE {keyspace}.subject_framework_course SET total_course = total_course + {courses} WHERE subject = '{subject}' AND framework = '{framework}'"
        session.execute(update_query)
        
    df = subject_framework_course(pandas_df)
    for index, row in df.iterrows():
        subject = row['subject']
        framework = row['framework']
        if framework=="":
            framework="None"
        count = row['count']
        execute_update_subject_framework_course(session,subject, framework, count, key_space)


def update_subject_tool_course(session,pandas_df,key_space):
    def subject_tool_course(df):
        df['tool'] = df['tool'].astype(str)

        # Phân tách chuỗi công cụ và sử dụng explode để tách thành từng dòng riêng biệt
        new_df = df.assign(tool=df['tool'].str.split(',')).explode('tool')

        # Nhóm theo chủ đề (subject) và công cụ (tool), sau đó đếm số lần xuất hiện
        return new_df.groupby(['subject', 'tool']).size().reset_index(name='count')

    def execute_update_subject_tool_course(session,subject, tool, courses, keyspace):
        update_query = f"UPDATE {keyspace}.subject_tool_course SET total_course = total_course + {courses} WHERE subject = '{subject}' AND tool = '{tool}'"
        session.execute(update_query)
# Hiển thị DataFrame mới
    df = subject_tool_course(pandas_df)
    for index, row in df.iterrows():
        subject = row['subject']
        tool = row['tool']
        if tool=="":
            tool="None"
        count = row['count']
        execute_update_subject_tool_course(session,subject, tool, count, key_space)

def update_top_tech(session,pandas_df,key_space):
    def top_tech(df):
        # Chuyển cột 'programing_language', 'tool' và 'framework' sang kiểu dữ liệu chuỗi (str)
        
        df['programming_language'] = df['programming_language'].astype(str)
        df['tool'] = df['tool'].astype(str)
        df['framework'] = df['framework'].astype(str)

        # Tạo DataFrame mới từ cột 'programing_language', 'tool' và 'framework'
        df_technology = pd.DataFrame(columns=['technology_type', 'technology_name'])

        # Thêm dữ liệu vào DataFrame mới từ cột 'progrmming_language'
        programing_language_df = df['programming_language'].str.split(',', expand=True)
        programing_language_df = programing_language_df.melt(var_name='technology_type', value_name='technology_name')
        programing_language_df = programing_language_df.dropna()
        programing_language_df['technology_type'] = 'programming_language'
        df_technology = pd.concat([df_technology, programing_language_df])

        # Thêm dữ liệu vào DataFrame mới từ cột 'tool'
        tool_df = df['tool'].str.split(',', expand=True)
        tool_df = tool_df.melt(var_name='technology_type', value_name='technology_name')
        tool_df = tool_df.dropna()
        tool_df['technology_type'] = 'tool'
        df_technology = pd.concat([df_technology, tool_df])

        # Thêm dữ liệu vào DataFrame mới từ cột 'framework'
        framework_df = df['framework'].str.split(',', expand=True)
        framework_df = framework_df.melt(var_name='technology_type', value_name='technology_name')
        framework_df = framework_df.dropna()
        framework_df['technology_type'] = 'framework'
        df_technology = pd.concat([df_technology, framework_df])

        # Đếm số lần xuất hiện của mỗi công nghệ (technology)
        return df_technology.groupby(['technology_type', 'technology_name']).size().reset_index(name='count')

    def execute_update_top_tech(session,type, name, courses, keyspace):
        update_query = f"UPDATE {keyspace}.top_tech SET total_course = total_course + {courses} WHERE tech_type = '{ type }' AND tech_name = '{ name }'"
        session.execute(update_query)
    df = top_tech(pandas_df)
    for index, row in df.iterrows():
        technology_type = row['technology_type']
        technology_name = row['technology_name']
        if technology_name=="":
            technology_name="None"
        if technology_type=="":
            technology_type="None"
        count = row['count']
        execute_update_top_tech(session,technology_type, technology_name, count, key_space)


def insert_cassandra_dw_jobposting(session,pandas_df,key_space):
    insert_query = session.prepare(f"""
    INSERT INTO {key_space}.jobposting
    ("link", "title", "industry", "description", "min_salary", "max_salary", "country", "programming_language", "tool", "framework","platform")
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

    for index, row in pandas_df.iterrows():
        session.execute(insert_query, (
            row['link'],row['title'],row['industry'], row['description'],row['min_salary'],
            row['max_salary'],row['country'],row['programming_language'],row['tool'],
            row['framework'],row['platform']
        ))

def update_industry_language_job(session,pandas_df,key_space):
    def industry_language_job(df):
        df['programming_language'] = df['programming_language'].astype(str)

        # Phân tách chuỗi programing_language và sử dụng explode để tách thành từng dòng riêng biệt
        new_df = df.assign(programing_language=df['programming_language'].str.split(',')).explode('programming_language')

    # Nhóm theo ngành (industry) và programing_language, sau đó đếm số lần xuất hiện
        return new_df.groupby(['industry', 'programming_language']).size().reset_index(name='count')
    
    def execute_update_industry_language_job(session,industry, language, jobs, keyspace):
        update_query = f"UPDATE {keyspace}.industry_language_job SET total_job = total_job + {jobs} WHERE industry = '{industry}' AND programming_language = '{language}'"
        session.execute(update_query)
        
    df = industry_language_job(pandas_df)
    for index, row in df.iterrows():
        industry = row['industry']
        programming_language = row['programming_language']
        count = row['count']
        execute_update_industry_language_job(session,industry, programming_language, count, key_space)

#### industry_framework_job
def update_industry_framework_job(session,pandas_df,key_space):
    def industry_framework_job(df):
        pandas_df['framework'] = pandas_df['framework'].astype(str)

    # Phân tách chuỗi framework và sử dụng explode để tách thành từng dòng riêng biệt
        new_df = pandas_df.assign(framework=pandas_df['framework'].str.split(',')).explode('framework')
        return new_df.groupby(['industry', 'framework']).size().reset_index(name='count')

    def execute_update_industry_framework_job(session,industry, framework, jobs, keyspace):
        update_query = f"UPDATE {keyspace}.industry_framework_job SET total_job = total_job + {jobs} WHERE industry = '{industry}' AND framework = '{framework}'"
        session.execute(update_query)
        
    df = industry_framework_job(pandas_df)
    for index, row in df.iterrows():
        industry = row['industry']
        framework = row['framework']
        count = row['count']
        execute_update_industry_framework_job(session,industry, framework, count, key_space)

def update_industry_tool_job(session,pandas_df,key_space):
    def industry_tool_job(df):
        df['tool'] = df['tool'].astype(str)

        # Phân tách chuỗi tool và sử dụng explode để tách thành từng dòng riêng biệt
        new_df = df.assign(tool=df['tool'].str.split(',')).explode('tool')

        # Nhóm theo ngành (industry) và tool, sau đó đếm số lần xuất hiện
        return new_df.groupby(['industry', 'tool']).size().reset_index(name='count')

    def execute_update_industry_tool_job(session,industry, tool, jobs, keyspace):
        update_query = f"UPDATE {keyspace}.industry_tool_job SET total_job = total_job + {jobs} WHERE industry = '{industry}' AND tool = '{tool}'"
        session.execute(update_query)
        
    df = industry_tool_job(pandas_df)
    for index, row in df.iterrows():
        industry = row['industry']
        tool = row['tool']
        count = row['count']
        execute_update_industry_tool_job(session,industry, tool, count, key_space)

def update_top_tech_job(session,pandas_df,key_space):
    def top_tech_job(df):
        # Chuyển cột 'programing_language', 'tool' và 'framework' sang kiểu dữ liệu chuỗi (str)
        df['programming_language'] = df['programming_language'].astype(str)
        df['tool'] = df['tool'].astype(str)
        df['framework'] = df['framework'].astype(str)

        # Tạo DataFrame mới từ cột 'programing_language', 'tool' và 'framework'
        df_technology = pd.DataFrame(columns=['technology_type', 'technology_name'])

        # Thêm dữ liệu vào DataFrame mới từ cột 'programing_language'
        programing_language_df = df['programming_language'].str.split(',', expand=True)
        programing_language_df = programing_language_df.melt(var_name='technology_type', value_name='technology_name')
        programing_language_df = programing_language_df.dropna()
        programing_language_df['technology_type'] = 'programming_language'
        df_technology = pd.concat([df_technology, programing_language_df])

        # Thêm dữ liệu vào DataFrame mới từ cột 'tool'
        tool_df = df['tool'].str.split(',', expand=True)
        tool_df = tool_df.melt(var_name='technology_type', value_name='technology_name')
        tool_df = tool_df.dropna()
        tool_df['technology_type'] = 'tool'
        df_technology = pd.concat([df_technology, tool_df])

        # Thêm dữ liệu vào DataFrame mới từ cột 'framework'
        framework_df = df['framework'].str.split(',', expand=True)
        framework_df = framework_df.melt(var_name='technology_type', value_name='technology_name')
        framework_df = framework_df.dropna()
        framework_df['technology_type'] = 'framework'
        df_technology = pd.concat([df_technology, framework_df])

        # Đếm số lần xuất hiện của mỗi công nghệ (technology)
        return df_technology.groupby(['technology_type', 'technology_name']).size().reset_index(name='count')

    def execute_update_top_tech_job(session,type, name, courses, keyspace):
        update_query = f"UPDATE {keyspace}.top_tech SET total_course = total_course + {courses} WHERE tech_type = '{ type }' AND tech_name = '{ name }'"
        session.execute(update_query)
    df = top_tech_job(pandas_df)
    for index, row in df.iterrows():
        technology_type = row['technology_type']
        technology_name = row['technology_name']
        count = row['count']
        execute_update_top_tech_job(session,technology_type, technology_name, count, key_space)

def update_industry_salary(session,pandas_df,key_space):
    def get_industry_salary(pandas_df):
        df = pandas_df[['industry','min_salary','max_salary']].query('min_salary > 0 and max_salary > 0')
        grouped = df.groupby('industry').mean()

        # Tính số lượng mỗi giá trị của cột "A" và lưu vào cột "D"
        grouped['jobs'] = df.groupby('industry').size()
        grouped=grouped.reset_index()
        return grouped
    
    
    def execute_update_industry_salary(session,key_space,industry, min_salary, max_salary, jobs):
        get_cur_salary = f"SELECT min_salary,max_salary, total_job FROM {key_space}.industry_job_salary WHERE industry = '{industry}'"
        result = session.execute(get_cur_salary).one()
        curmin = result.min_salary if result else 0.0
        curmax = result.max_salary if result else 0.0
        curtotaljobs = result.total_job if result else 0
        total = curtotaljobs + jobs
        averagemin = (curmin*curtotaljobs + min_salary*jobs)/total
        averagemax = (curmax*curtotaljobs + max_salary*jobs)/total
        update_query = f"UPDATE {key_space}.industry_job_salary SET min_salary = {averagemin}, max_salary = {averagemax}, total_job = {total} WHERE industry = '{industry}'"
        session.execute(update_query)

    df = get_industry_salary(pandas_df)
    for index, row in df.iterrows():
        industry = row['industry']
        min_salary = row['min_salary']
        max_salary = row['max_salary']        
        jobs = row['jobs']
        execute_update_industry_salary(session,key_space,industry, min_salary, max_salary, jobs)
    








