from application import app
from flask import render_template, request
import pandas as pd
import json
import plotly
import plotly.express as px
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# bundlepath = 'application/secure-connect-datn.zip'
# clientId = 'PKUrqlvgFpAippRORPhslTCP'
# clientSecret = '4NnPaNpsB8Z_SpdTUX7hFy3UHmCfZtmB_Xrs7K21,WmxMJTTjzmZXqfwFNqTETCZ6f2nj6sL_h1KNc6xrokZtkIUlekoraJiZygUBlcrvlNKfQ13KtkbIvuv4qm9ZSWZ'

# cloud_config= {
#     'secure_connect_bundle': bundlepath
# }
# auth_provider = PlainTextAuthProvider(clientId, clientSecret)
cluster = Cluster(['127.0.0.1'])

app.config['ACTIVE_TAB'] = '/'


list_course_keyspace = ['datawarehouse','coursera','udemy','edx' ]
list_jobposting_keyspace = ['datawarehouse', 'nodeflair','dice' ]

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/data')
def data():
    source_data = [
        {
            'title' : 'Nguồn dữ liệu tuyển dụng',
            'data' : [
                {
                    'title': 'NodeFlair',
                    'information':[
                        {
                            'title': 'Người dùng',
                            'value': '4,000+'
                        },
                        {
                            'title': 'Công ty',
                            'value': '5,000+'
                        },
                        {
                            'title': 'Đối tác',
                            'value': '200+'
                        }
                    ],
                    'website': 'https://nodeflair.com/'
                },
                {
                    'title': 'Dice',
                    'information':[
                        {
                            'title': 'Người dùng',
                            'value': '5.9 M'
                        },
                        {
                            'title': 'Người dùng/tháng',
                            'value': '1.7 M'
                        },
                        {
                            'title': 'Hồ sơ',
                            'value': '3 M'
                        }
                    ],
                    'website': 'https://www.dice.com/'
                }
            ]
        },

        {
            'title' : 'Nguồn dữ liệu khóa học',
        
            'data' : [
                {
                    'title': 'Coursera',
                    'information':[
                        {
                            'title': 'Khóa học',
                            'value': '4,000+'
                        },
                        {
                            'title': 'Người dùng',
                            'value': '82+ M'
                        },
                        {
                            'title': 'Đối tác',
                            'value': '200+'
                        }
                    ],
                    'website': 'https://www.coursera.org/?irclickid=wlvRjDSqHxyNUeaU9XwM0VgNUkASXR1pbw4DUw0&irgwc=1&utm_medium=partners&utm_source=impact&utm_campaign=2985301&utm_content=b2c'
                },
                {
                    'title': 'Udemy',
                    'information':[
                        {
                            'title': 'Khóa học',
                            'value': '65,000+'
                        },
                        {
                            'title': 'Người dùng',
                            'value': '20+ M'
                        },
                        {
                            'title': 'Quốc gia',
                            'value': '190+'
                        }
                    ],
                    'website': 'https://www.udemy.com/?ranMID=39197&ranEAID=yNfEamYSgXk&ranSiteID=yNfEamYSgXk-Q716xy3YHMHwt8IcQqpOnA&LSNPUBID=yNfEamYSgXk&utm_source=aff-campaign&utm_medium=udemyads&gclid=Cj0KCQjwyLGjBhDKARIsAFRNgW8Ng4w4tvTFvPbBKcXsbBwCk4SyZdluQ97e3dvxkn5X7DtUWHJqyQAaAv0KEALw_wcB'
                },
                {
                    'title': 'edX',
                    'information':[
                        {
                            'title': 'Khóa học',
                            'value': '3,000+'
                        },
                        {
                            'title': 'Người dùng',
                            'value': '38+ M'
                        },
                        {
                            'title': 'Đối tác',
                            'value': '120+'
                        }
                    ],
                    'website': 'https://www.edx.org/?irclickid=wlvRjDSqHxyNUeaU9XwM0VgNUkASXW0Zbw4DUw0&utm_source=affiliate&utm_medium=Ecom%20EWAY&utm_campaign=Online%20Tracking%20Link_&utm_content=ONLINE_TRACKING_LINK&irgwc=1'
                }
            ]
        }
    ]
    return render_template('data.html', source_data = source_data)

@app.route('/about')
def about():
    team_members = [
        {
            'name': 'Nguyễn Trần Minh Thư',
            'role': 'Giáo viên hướng dẫn',
            'image': '../static/img/businesswoman.png',
            'information':[
                {
                    'title': 'Trường',
                    'value': 'Đại học KHTN, ĐHQG-HCM'
                },
                {
                    'title': 'Khoa',
                    'value': 'Công nghệ thông tin'
                },
                {
                    'title': 'Học vị',
                    'value': 'Tiến sĩ'
                },
                {
                    'title': 'Nghề nghiệp',
                    'value': 'Giảng viên'
                },
                {
                    'title': 'Chuyên ngành',
                    'value': 'Hệ thống thông tin'
                },
                {
                    'title': 'Email',
                    'value': 'ntmthu@fit.hcmus.edu.vn'
                },
                {
                    'title': 'Sở thích',
                    'value': 'Đọc sách, Nấu ăn'
                }
            ]
        },
        {
            'name': 'Nguyễn Phạm Quang Dũng',
            'role': 'Trưởng nhóm',
            'image': '../static/img/man.png',
            'information':[
                {
                    'title': 'Trường',
                    'value': 'Đại học KHTN, ĐHQG-HCM'
                },
                {
                    'title': 'Khoa',
                    'value': 'Công nghệ thông tin'
                },
                {
                    'title': 'MSSV',
                    'value': '19120485'
                },
                {
                    'title': 'Chuyên ngành',
                    'value': 'Khoa học dữ liệu'
                },
                {
                    'title': 'Email',
                    'value': 'npqdung17@gmail.com'
                },
                {
                    'title': 'Định hướng',
                    'value': 'Data Engineer'
                },
                {
                    'title': 'Sở thích',
                    'value': 'Thể thao, Nấu ăn'
                }
            ]
        },
        {
            'name': 'Dương Thanh Hiệp',
            'role': 'Thành viên',
            'image': '../static/img/hacker.png',
            'information':[
                {
                    'title': 'Trường',
                    'value': 'Đại học KHTN, ĐHQG-HCM'
                },
                {
                    'title': 'Khoa',
                    'value': 'Công nghệ thông tin'
                },
                {
                    'title': 'MSSV',
                    'value': '19120505'
                },
                {
                    'title': 'Chuyên ngành',
                    'value': 'Khoa học máy tính'
                },
                {
                    'title': 'Email',
                    'value': 'thanhhiep0705@gmail.com'
                },
                {
                    'title': 'Định hướng',
                    'value': 'Web Developer'
                },
                {
                    'title': 'Sở thích',
                    'value': 'Du lịch, Thể thao'
                }
            ]
        },
        {
            'name': 'Nguyễn Thị Tiểu Mi',
            'role': 'Thành viên',
            'image': '../static/img/woman.png',
            'information':[
                {
                    'title': 'Trường',
                    'value': 'Đại học KHTN, ĐHQG-HCM'
                },
                {
                    'title': 'Khoa',
                    'value': 'Công nghệ thông tin'
                },
                {
                    'title': 'MSSV',
                    'value': '19120577'
                },
                {
                    'title': 'Chuyên ngành',
                    'value': 'Hệ thống thông tin'
                },
                {
                    'title': 'Email',
                    'value': 'tieumi2509@gmail.com'
                },
                {
                    'title': 'Định hướng',
                    'value': 'Web Developer'
                },
                {
                    'title': 'Sở thích',
                    'value': 'Đọc sách, Nấu ăn'
                }
            ]
        },
        {
            'name': 'Lê Thành Lộc',
            'role': 'Thành viên',
            'image': '../static/img/businessman.png',
            'information':[
                {
                    'title': 'Trường',
                    'value': 'Đại học KHTN, ĐHQG-HCM'
                },
                {
                    'title': 'Khoa',
                    'value': 'Công nghệ thông tin'
                },
                {
                    'title': 'MSSV',
                    'value': '19120562'
                },
                {
                    'title': 'Chuyên ngành',
                    'value': 'Khoa học máy tính'
                },
                {
                    'title': 'Email',
                    'value': 'lochcmus@gmail.com'
                },
                {
                    'title': 'Định hướng',
                    'value': 'Data Engineer'
                },
                {
                    'title': 'Sở thích',
                    'value': 'Hát, Du lịch'
                }
            ]
        },
        {
            'name': 'Nguyễn Thị Kim Ngân',
            'role': 'Thành viên',
            'image': '../static/img/girl.png',
            'information':[
                {
                    'title': 'Trường',
                    'value': 'Đại học KHTN, ĐHQG-HCM'
                },
                {
                    'title': 'Khoa',
                    'value': 'Công nghệ thông tin'
                },
                {
                    'title': 'MSSV',
                    'value': '19120598'
                },
                {
                    'title': 'Chuyên ngành',
                    'value': 'Hệ thống thông tin'
                },
                {
                    'title': 'Email',
                    'value': 'ntkn.mnkt@gmail.com'
                },
                {
                    'title': 'Định hướng',
                    'value': 'Web Developer'
                },
                {
                    'title': 'Sở thích',
                    'value': 'Đọc sách, Thể thao'
                }
            ]
        }
    ]
    return render_template('about.html', team_members=team_members)

@app.route('/course-finder', methods=['GET', 'POST'])
def courseFinder():
    keyspace = 'datawarehouse'
    session = cluster.connect()
    
    selected_options = request.form.getlist('selected_options')

    for key in selected_options:
        if key.find('Nhà cung cấp') != -1:
            keyspace = key.split(':')[1].strip().lower()
            break
    

    query = f'select name, subject, enroll, programming_language, fee, framework, tool, platform, level, rating, link from {keyspace}.course;'
    rs = session.execute(query)
    df = pd.DataFrame(list(rs))
    df['fee'] = df['fee'].round(2)
    df['rating'] = df['rating'].round(1)

    courses = pd.DataFrame([])
    
    if len(selected_options) != 0:
        courses = courseFilter(selected_options,df)

    top_course = getTopCourses(df)

    filter_data = getCourseFilterData(df)

    return render_template('course-finder.html', courses = courses , top_course = top_course, selected_options = selected_options,filter_data = filter_data)

@app.route('/job-finder', methods=['GET', 'POST'])
def jobFinder():
    keyspace = 'datawarehouse'
    session = cluster.connect()
    selected_options = request.form.getlist('selected_options')

    query = f'select title, industry, tool, programming_language, platform, min_salary, max_salary, framework, link from {keyspace}.jobposting;'
    rs = session.execute(query)
    df = pd.DataFrame(list(rs))
    df['min_salary'] = df['min_salary'].round(2)
    df['max_salary'] = df['max_salary'].round(2)

    filtered_jobs = pd.DataFrame([])
    
    if len(selected_options) != 0:
        filtered_jobs = jobFilter(selected_options,df)

    top_jobs = getTopJob(df)

    filter_data = getJobFilterData(df)

    return render_template('job-finder.html', filtered_jobs = filtered_jobs , top_jobs = top_jobs, selected_options = selected_options,filter_data = filter_data)

@app.route('/course-visualization/<keyspace>', methods=['GET', 'POST'])
def courseVisualization(keyspace):
    session = cluster.connect()

    db_info = course_data_statistic(session, keyspace)

    fig1, fig1_dialog = course_graph1(session, keyspace)
    graph1JSON = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graph1_dialogJSON = json.dumps(fig1_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig2, fig2_dialog = course_graph2(session, keyspace)
    graph2JSON = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)
    graph2_dialogJSON = json.dumps(fig2_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig3, fig3_dialog,listSubjects3  = course_graph3(session, keyspace)
    graph3JSON = json.dumps(fig3, cls=plotly.utils.PlotlyJSONEncoder)
    graph3_dialogJSON = json.dumps(fig3_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig4, fig4_dialog,listSubjects4  = course_graph4(session, keyspace)
    graph4JSON = json.dumps(fig4, cls=plotly.utils.PlotlyJSONEncoder)
    graph4_dialogJSON = json.dumps(fig4_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig5, fig5_dialog,listSubjects5  = course_graph5(session, keyspace)
    graph5JSON = json.dumps(fig5, cls=plotly.utils.PlotlyJSONEncoder)
    graph5_dialogJSON = json.dumps(fig5_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig6, fig6_dialog = course_graph6(session, keyspace)
    graph6JSON = json.dumps(fig6, cls=plotly.utils.PlotlyJSONEncoder)
    graph6_dialogJSON = json.dumps(fig6_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig7, fig7_dialog = course_graph7(session, keyspace)
    graph7JSON = json.dumps(fig7, cls=plotly.utils.PlotlyJSONEncoder)
    graph7_dialogJSON = json.dumps(fig7_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig8, fig8_dialog = course_graph8(session, keyspace)
    graph8JSON = json.dumps(fig8, cls=plotly.utils.PlotlyJSONEncoder)
    graph8_dialogJSON = json.dumps(fig8_dialog, cls=plotly.utils.PlotlyJSONEncoder)


    fig9, fig9_dialog,listFramework9  = course_graph9(session, keyspace)
    graph9JSON = json.dumps(fig9, cls=plotly.utils.PlotlyJSONEncoder)
    graph9_dialogJSON = json.dumps(fig9_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig10, fig10_dialog,listFramework10  = course_graph10(session, keyspace)
    graph10JSON = json.dumps(fig10, cls=plotly.utils.PlotlyJSONEncoder)
    graph10_dialogJSON = json.dumps(fig10_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig11, fig11_dialog,listFramework11  = course_graph11(session, keyspace)
    graph11JSON = json.dumps(fig11, cls=plotly.utils.PlotlyJSONEncoder)
    graph11_dialogJSON = json.dumps(fig11_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    graphsData = [
        {
            'groupName': 'Thống kê theo lĩnh vực',
            'data': [
                {
                    'name':'Biểu đồ thống kê số lượng học viên theo lĩnh vực',
                    'chartId':'chart1',
                    'dialogId': 'dialog-chart1',
                    'graphData': graph1JSON,
                    'graphDialogData': graph1_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                },
                {
                    'name':'Biểu đồ thống kê số lượng khóa học theo lĩnh vực',
                    'chartId':'chart2',
                    'dialogId': 'dialog-chart2',
                    'graphData': graph2JSON,
                    'graphDialogData': graph2_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                }, 
                {
                    'name':'Biểu đồ tương quan tỷ lệ ngôn ngữ lập trình theo lĩnh vực',
                    'chartId':'chart3',
                    'dialogId': 'dialog-chart3',
                    'graphData': graph3JSON,
                    'graphDialogData': graph3_dialogJSON,
                    'haveFilter': True,
                    'filter': listSubjects3
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ khung chương trình theo lĩnh vực',
                    'chartId':'chart4',
                    'dialogId': 'dialog-chart4',
                    'graphData': graph4JSON,
                    'graphDialogData': graph4_dialogJSON,
                    'haveFilter': True,
                    'filter': listSubjects4
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ công cụ trình theo lĩnh vực',
                    'chartId':'chart5',
                    'dialogId': 'dialog-chart5',
                    'graphData': graph5JSON,
                    'graphDialogData': graph5_dialogJSON,
                    'haveFilter': True,
                    'filter': listSubjects5
                },
            ]
        },
        {
            'groupName': 'Thống kê theo cấp độ',
            'data': [
                {
                    'name':'Biểu đồ thời gian học trung bình theo lĩnh vực và cấp độ',
                    'chartId':'chart6',
                    'dialogId': 'dialog-chart6',
                    'graphData': graph6JSON,
                    'graphDialogData': graph6_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                },
                {
                    'name':'Biểu đồ học phí trung bình theo lĩnh vực và cấp độ',
                    'chartId':'chart7',
                    'dialogId': 'dialog-chart7',
                    'graphData': graph7JSON,
                    'graphDialogData': graph7_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                }
            ]
        },
        {
            'groupName': 'Thống kê theo công nghệ',
            'data': [
                {
                    'name':'Biểu đồ thống kê mức độ phổ biến các công nghệ hiện nay',
                    'chartId':'chart8',
                    'dialogId': 'dialog-chart8',
                    'graphData': graph8JSON,
                    'graphDialogData': graph8_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ lĩnh vực theo ngôn ngữ lập trình',
                    'chartId':'chart9',
                    'dialogId': 'dialog-chart9',
                    'graphData': graph9JSON,
                    'graphDialogData': graph9_dialogJSON,
                    'haveFilter': True,
                    'filter': listFramework9
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ lĩnh vực theo khung chương trình',
                    'chartId':'chart10',
                    'dialogId': 'dialog-chart10',
                    'graphData': graph10JSON,
                    'graphDialogData': graph10_dialogJSON,
                    'haveFilter': True,
                    'filter': listFramework10
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ lĩnh vực theo công cụ lập trình',
                    'chartId':'chart11',
                    'dialogId': 'dialog-chart11',
                    'graphData': graph11JSON,
                    'graphDialogData': graph11_dialogJSON,
                    'haveFilter': True,
                    'filter': listFramework11
                }
            ]
        }
    ]

    return render_template('course-visualization.html', graphsData=graphsData, db_info=db_info, selected_db=keyspace)

@app.route('/job-visualization/<keyspace>' , methods=['GET', 'POST'])
def jobVisualization(keyspace):
    session = cluster.connect()

    db_info = job_data_statistic(session, keyspace)

    fig1, fig1_dialog = job_graph1(session, keyspace)
    graph1JSON = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)
    graph1_dialogJSON = json.dumps(fig1_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig2, fig2_dialog = job_graph2(session, keyspace)
    graph2JSON = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)
    graph2_dialogJSON = json.dumps(fig2_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig3, fig3_dialog,listSubjects3  = job_graph3(session, keyspace)
    graph3JSON = json.dumps(fig3, cls=plotly.utils.PlotlyJSONEncoder)
    graph3_dialogJSON = json.dumps(fig3_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig4, fig4_dialog,listSubjects4  = job_graph4(session, keyspace)
    graph4JSON = json.dumps(fig4, cls=plotly.utils.PlotlyJSONEncoder)
    graph4_dialogJSON = json.dumps(fig4_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig5, fig5_dialog,listSubjects5  = job_graph5(session, keyspace)
    graph5JSON = json.dumps(fig5, cls=plotly.utils.PlotlyJSONEncoder)
    graph5_dialogJSON = json.dumps(fig5_dialog, cls=plotly.utils.PlotlyJSONEncoder)


    fig6, fig6_dialog = job_graph6(session, keyspace)
    graph6JSON = json.dumps(fig6, cls=plotly.utils.PlotlyJSONEncoder)
    graph6_dialogJSON = json.dumps(fig6_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig7, fig7_dialog,listLanguages7  = job_graph7(session, keyspace)
    graph7JSON = json.dumps(fig7, cls=plotly.utils.PlotlyJSONEncoder)
    graph7_dialogJSON = json.dumps(fig7_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig8, fig8_dialog,listFrameworks8  = job_graph8(session, keyspace)
    graph8JSON = json.dumps(fig8, cls=plotly.utils.PlotlyJSONEncoder)
    graph8_dialogJSON = json.dumps(fig8_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    fig9, fig9_dialog,listTools9  = job_graph9(session, keyspace)
    graph9JSON = json.dumps(fig9, cls=plotly.utils.PlotlyJSONEncoder)
    graph9_dialogJSON = json.dumps(fig9_dialog, cls=plotly.utils.PlotlyJSONEncoder)

    graphsData = [
        {
            'groupName': 'Thống kê theo lĩnh vực',
            'data': [
                {
                    'name':'Biểu đồ thống kê mức độ phổ biến của các lĩnh vực',
                    'chartId':'chart1',
                    'dialogId': 'dialog-chart1',
                    'graphData': graph1JSON,
                    'graphDialogData': graph1_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                },
                {
                    'name':'Biểu đồ phân bố mức lương theo lĩnh vực',
                    'chartId':'chart2',
                    'dialogId': 'dialog-chart2',
                    'graphData': graph2JSON,
                    'graphDialogData': graph2_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                },
                                {
                    'name':'Biểu đồ tương quan tỷ lệ ngôn ngữ lập trình theo lĩnh vực',
                    'chartId':'chart3',
                    'dialogId': 'dialog-chart3',
                    'graphData': graph3JSON,
                    'graphDialogData': graph3_dialogJSON,
                    'haveFilter': True,
                    'filter': listSubjects3
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ khung chương trình theo lĩnh vực',
                    'chartId':'chart4',
                    'dialogId': 'dialog-chart4',
                    'graphData': graph4JSON,
                    'graphDialogData': graph4_dialogJSON,
                    'haveFilter': True,
                    'filter': listSubjects4
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ công cụ trình theo lĩnh vực',
                    'chartId':'chart5',
                    'dialogId': 'dialog-chart5',
                    'graphData': graph5JSON,
                    'graphDialogData': graph5_dialogJSON,
                    'haveFilter': True,
                    'filter': listSubjects5
                },
            ]
        },
        {
            'groupName': 'Thống kê theo công nghệ',
            'data': [
                {
                    'name':'Biểu đồ thống kê mức độ phổ biến các công nghệ hiện nay',
                    'chartId':'chart6',
                    'dialogId': 'dialog-chart6',
                    'graphData': graph6JSON,
                    'graphDialogData': graph6_dialogJSON,
                    'haveFilter': False,
                    'filter': []
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ lĩnh vực theo ngôn ngữ lập trình',
                    'chartId':'chart7',
                    'dialogId': 'dialog-chart7',
                    'graphData': graph7JSON,
                    'graphDialogData': graph7_dialogJSON,
                    'haveFilter': True,
                    'filter': listLanguages7
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ lĩnh vực theo khung chương trình',
                    'chartId':'chart8',
                    'dialogId': 'dialog-chart8',
                    'graphData': graph8JSON,
                    'graphDialogData': graph8_dialogJSON,
                    'haveFilter': True,
                    'filter': listFrameworks8
                },
                {
                    'name':'Biểu đồ tương quan tỷ lệ lĩnh vực theo công cụ lập trình',
                    'chartId':'chart9',
                    'dialogId': 'dialog-chart9',
                    'graphData': graph9JSON,
                    'graphDialogData': graph9_dialogJSON,
                    'haveFilter': True,
                    'filter': listTools9
                },
            ]
        }
    ]

    return render_template('job-visualization.html', graphsData=graphsData, db_info=db_info, selected_db = keyspace)

def getTopCourses(df):
    df_top = df.sort_values(by = 'enroll', ascending=False).head(8)
    return df_top

def courseFilter(options, df):
    subjects = []
    frameworks = []
    programming_languages = []
    levels = []
    tools = []
    platforms = []
    for key in options:
        if key.find('Lĩnh vực') != -1:
            value = key.split(':')[1].strip()
            subjects.append(value)
        if key.find('Khung chương trình') != - 1:
            value = key.split(':')[1].strip()
            frameworks.append(value)
        if key.find('NN lập trình') != - 1:
            value = key.split(':')[1].strip()
            programming_languages.append(value)
        if key.find('Cấp độ') != - 1:
            value = key.split(':')[1].strip()
            levels.append(value)
        if key.find('Công cụ') != - 1:
            value = key.split(':')[1].strip()
            tools.append(value)
        if key.find('Nền tảng') != - 1:
            value = key.split(':')[1].strip()
            platforms.append(value)

    conditions = []
    if subjects:
        conditions.append(df['subject'].isin(subjects))
    if levels:
        conditions.append(df['level'].isin(levels))
    if programming_languages:
        programming_language_condition = df['programming_language'].apply(lambda x: any(language in x.split(',') for language in programming_languages))
        conditions.append(programming_language_condition)
    if frameworks:
        framework_condition = df['framework'].apply(lambda x: any(framework in x.split(',') for framework in frameworks))
        conditions.append(framework_condition)
    if tools:
        tool_condition = df['tool'].apply(lambda x: any(tool in x.split(',') for tool in tools))
        conditions.append(tool_condition)
    if platforms:
        platform_condition = df['platform'].apply(lambda x: any(platform in x.split(',') for platform in platforms))
        conditions.append(platform_condition)

    # Áp dụng các điều kiện tìm kiếm (nếu có ít nhất một điều kiện)
    if conditions:
        filtered_df = df[conditions[0]]
        for condition in conditions[1:]:
            filtered_df = filtered_df[condition]
    else:
        filtered_df = df

    return filtered_df.sort_values(by='enroll', ascending=False)

def getNormalizedArray(array):
    result = []
    for row in array:
        rs=[]
        if row != None:
            if row.find(',') != -1:
                rs = row.split(",")
                rs = list(map(str.strip, rs))
                for item in rs:
                    if item.strip() not in result:
                        result.append(item)
            else: 
                if row.strip() not in result:
                    result.append(row.strip())

    return sorted(result)

def getCourseFilterData(df):
    subjects = sorted([x for x in df.subject.unique()])
    levels = sorted([x for x in df.level.unique()])
    frameworks = getNormalizedArray(df.framework.unique())
    languages = getNormalizedArray(df.programming_language.unique())
    tools = getNormalizedArray(df.tool.unique())
    platforms = getNormalizedArray(df.platform.unique())

    filter_data = [
        {
            'name': 'Nhà cung cấp',
            'value': ['Coursera', 'Udemy', 'Edx']
        },
        {
            'name': 'Lĩnh vực',
            'value': subjects
        },
        {
            'name': 'Cấp độ',
            'value': levels
        },
        {
            'name': 'Khung chương trình',
            'value': frameworks
        },
        {
            'name': 'NN lập trình',
            'value': languages
        },
        {
            'name': 'Công cụ',
            'value': tools
        },
        {
            'name': 'Nền tảng',
            'value': platforms
        }
    ]
    return filter_data

def course_data_statistic(session, keyspace):
    query = f'select subject, programming_language, tool, framework, platform, link from {keyspace}.course;'
    rs = session.execute(query)
    df = pd.DataFrame(list(rs))
    total_course = len(df)
    total_subject = df[df['subject'] != 'None']['subject'].nunique()
    total_framework = df['framework'].str.split(',').explode().str.strip()
    total_framework = total_framework[total_framework != 'None'].nunique()
    total_tool = df['tool'].str.split(',').explode().str.strip()
    total_tool = total_tool[total_tool != 'None'].nunique()
    total_language = df['programming_language'].str.split(',').explode().str.strip()
    total_language = total_language[total_language != 'None'].nunique()
    total_platform = df['platform'].str.split(',').explode().str.strip()
    total_platform = total_platform[total_platform != 'None'].nunique()
    return [total_course,total_subject, total_language, total_tool, total_framework, total_platform]

def getTopJob(df):
    df_top = df.sort_values(by = 'max_salary', ascending=False).head(10)
    return df_top

def jobFilter(options, df):
    industries = []
    frameworks = []
    programming_languages = []
    tools = []
    platforms = []

    for key in options:
        if key.find('Lĩnh vực') != -1:
            value = key.split(':')[1].strip()
            industries.append(value)
        if key.find('Khung chương trình') != - 1:
            value = key.split(':')[1].strip()
            frameworks.append(value)
        if key.find('NN lập trình') != - 1:
            value = key.split(':')[1].strip()
            programming_languages.append(value)
        if key.find('Công cụ') != - 1:
            value = key.split(':')[1].strip()
            tools.append(value)
        if key.find('Nền tảng') != - 1:
            value = key.split(':')[1].strip()
            platforms.append(value)

    conditions = []
    if industries:
        conditions.append(df['industry'].isin(industries))
    if programming_languages:
        programming_language_condition = df['programming_language'].apply(lambda x: any(language in x.split(',') for language in programming_languages))
        conditions.append(programming_language_condition)
    if frameworks:
        framework_condition = df['framework'].apply(lambda x: any(framework in x.split(',') for framework in frameworks))
        conditions.append(framework_condition)
    if tools:
        tool_condition = df['tool'].apply(lambda x: any(tool in x.split(',') for tool in tools))
        conditions.append(tool_condition)
    if platforms:
        platform_condition = df['platform'].apply(lambda x: any(platform in x.split(',') for platform in platforms))
        conditions.append(platform_condition)

    if conditions:
        filtered_df = df[conditions[0]]
        for condition in conditions[1:]:
            filtered_df = filtered_df[condition]
    else:
        filtered_df = df

    return filtered_df

def getJobFilterData(df):
    industries = sorted([x for x in df.industry.unique()])
    frameworks = getNormalizedArray(df.framework.unique())
    languages = getNormalizedArray(df.programming_language.unique())
    tools = getNormalizedArray(df.tool.unique())
    platforms = getNormalizedArray(df.platform.unique())

    filter_data = [
        {
            'name': 'Lĩnh vực',
            'value': industries
        },
        {
            'name': 'Khung chương trình',
            'value': frameworks
        },
        {
            'name': 'NN lập trình',
            'value': languages
        },
        {
            'name': 'Công cụ',
            'value': tools
        },
        {
            'name': 'Nền tảng',
            'value': platforms
        },
    ]
    return filter_data

def job_data_statistic(session, keyspace):
    query = f'select industry, programming_language, tool, framework, platform, link from {keyspace}.jobposting;'
    rs = session.execute(query)
    df = pd.DataFrame(list(rs))
    total_course = len(df)
    total_industry = df[df['industry'] != 'None']['industry'].nunique()
    total_framework = df['framework'].str.split(',').explode().str.strip()
    total_framework = total_framework[total_framework != 'None'].nunique()
    total_tool = df['tool'].str.split(',').explode().str.strip()
    total_tool = total_tool[total_tool != 'None'].nunique()
    total_language = df['programming_language'].str.split(',').explode().str.strip()
    total_language = total_language[total_language != 'None'].nunique()
    total_platform = df['platform'].str.split(',').explode().str.strip()
    total_platform = total_platform[total_platform != 'None'].nunique()
    return [total_course,total_industry, total_language, total_tool, total_framework, total_platform]

def course_graph1(session, keyspace):
    query = f'select subject, total_enroll from {keyspace}.subject_course_enroll;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Total Enrolls']
    df = df.loc[df['Subject'] != 'None']
    fig = px.pie(df, names="Subject", values="Total Enrolls", height= 420, width= 420, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig.update_layout(showlegend=False,margin=dict(t=15, l=0))
    fig.update_traces(textposition='inside', textinfo='label + value')
    
    fig_dialog = px.pie(df, names="Subject", values="Total Enrolls", height=750, width= 1000, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
    return fig, fig_dialog

def course_graph2(session, keyspace):
    query = f'select subject, total_course from {keyspace}.subject_course_enroll;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Total Courses']
    df = df.loc[df['Subject'] != 'None']
    fig = px.pie(df.head(8), names="Subject", values="Total Courses",height= 420, width= 420, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig.update_layout(showlegend=False,margin=dict(t=15, l=0))
    fig.update_traces(textposition='inside', textinfo='label + value')

    fig_dialog = px.pie(df, names="Subject", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
    return fig, fig_dialog

def course_graph3(session, keyspace):
    query = f'select subject,programming_language,total_course from {keyspace}.subject_language_course;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Language', 'Total Courses']
    df = df.loc[df['Subject'] != 'None']
    subjects_list = df.groupby('Subject')['Total Courses'].sum().nlargest(10).index.tolist()
    subjects_list.sort()
    list_fig = []
    list_fig_dialog = []
    for subject in subjects_list:
        df_temp = df[df['Subject'] == subject]
        fig = px.pie(df_temp.head(8), names="Language", values="Total Courses", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Language", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(subjects_list))

def course_graph4(session, keyspace):
    query = f'select subject,framework,total_course from {keyspace}.subject_framework_course;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Framework', 'Total Courses']
    df = df.loc[df['Subject'] != 'None']
    subjects_list = df.groupby('Subject')['Total Courses'].sum().nlargest(10).index.tolist()
    subjects_list.sort()
    list_fig = []
    list_fig_dialog = []
    for subject in subjects_list:
        df_temp = df[df['Subject'] == subject]
        fig = px.pie(df_temp.head(8), names="Framework", values="Total Courses", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Framework", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(subjects_list))

def course_graph5(session, keyspace):
    query = f'select subject,tool,total_course from {keyspace}.subject_tool_course;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Tool', 'Total Courses']
    df = df.loc[df['Subject'] != 'None']
    subjects_list = df.groupby('Subject')['Total Courses'].sum().nlargest(10).index.tolist()
    subjects_list.sort()
    list_fig = []
    list_fig_dialog = []
    for subject in subjects_list:
        df_temp = df[df['Subject'] == subject]
        fig = px.pie(df_temp.head(8), names="Tool", values="Total Courses", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Tool", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(subjects_list))

def course_graph6(session, keyspace):
    query = f'select subject, level, time from {keyspace}.subject_level_time_fee;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Level', 'Average Time']
    df = df.loc[df['Subject'] != 'None']
    fig = px.bar(df.head(8), x='Subject', y='Average Time', color='Level', barmode='stack', height=410, width=375, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig.update_layout(margin=dict(t=5, l=0))
    fig.update_traces(texttemplate='%{y}', textposition='inside')

    fig_dialog = px.bar(df, x='Subject', y='Average Time', color='Level', barmode='stack', height=650, width=1000,color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig_dialog.update_traces(texttemplate='%{y}', textposition='inside')
    
    return fig, fig_dialog

def course_graph7(session, keyspace):
    query = f'select subject, level, fee from {keyspace}.subject_level_time_fee;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Level', 'Average Fee']
    df = df.loc[df['Subject'] != 'None']
    fig = px.bar(df.head(8), x='Subject', y='Average Fee', color='Level', barmode='stack', height=410, width=375, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig.update_layout( margin=dict(t=5, l=0))
    fig.update_traces(texttemplate='%{y}', textposition='auto')
    fig_dialog = px.bar(df, x='Subject', y='Average Fee', color='Level', barmode='stack', height=650, width=1100,color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig_dialog.update_traces(texttemplate='%{y}', textposition='inside')
    return fig, fig_dialog

def course_graph8(session, keyspace):
    query = f'select * from {keyspace}.top_tech;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Technology Type', 'Technology Name','Total Courses']
    df = df.loc[df['Technology Name'] != 'None']
    df_limited = df.groupby('Technology Type').apply(lambda x: x.nlargest(10, 'Total Courses')).reset_index(drop=True)
    fig = px.bar(df_limited.head(8), x='Technology Type', y='Total Courses', color='Technology Name', barmode='group', height=410, width=375, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig.update_layout(margin=dict(t=5, l=0))
    fig.update_traces(texttemplate='%{y}', textposition='auto')

    fig_dialog = px.bar(df_limited, x='Technology Type', y='Total Courses', color='Technology Name', barmode='group', height= 650, width =1100, color_discrete_sequence=px.colors.qualitative.Pastel1)
    fig_dialog.update_layout(xaxis_title='Technology Type', yaxis_title='Total Courses')
    fig_dialog.update_traces(texttemplate='%{y}', textposition='inside')
    return fig, fig_dialog


def course_graph9(session, keyspace):
    query = f'select subject,programming_language,total_course from {keyspace}.subject_language_course;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Language', 'Total Courses']
    filtered_df = df.loc[df['Language'] != 'None']
    languages_list = filtered_df.groupby('Language')['Total Courses'].sum().nlargest(10).index.tolist()
    languages_list.sort()
    list_fig = []
    list_fig_dialog = []
    for language in languages_list:
        df_temp = df[df['Language'] == language]
        fig = px.pie(df_temp.head(8), names="Subject", values="Total Courses", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Subject", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)


    return list_fig ,list_fig_dialog, list(enumerate(languages_list))

def course_graph10(session, keyspace):
    query = f'select subject,framework,total_course from {keyspace}.subject_framework_course;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Framework', 'Total Courses']
    filtered_df = df.loc[df['Framework'] != 'None']
    frameworks_list = filtered_df.groupby('Framework')['Total Courses'].sum().nlargest(10).index.tolist()
    frameworks_list.sort()
    list_fig = []
    list_fig_dialog = []
    for framework in frameworks_list:
        df_temp = df[df['Framework'] == framework]
        fig = px.pie(df_temp.head(8), names="Subject", values="Total Courses", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Subject", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel1)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(frameworks_list))

def course_graph11(session, keyspace):
    query = f'select subject,tool,total_course from {keyspace}.subject_tool_course;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Subject', 'Tool', 'Total Courses']
    df = df.loc[df['Tool'] != 'None']
    tools_list = df.groupby('Tool')['Total Courses'].sum().nlargest(10).index.tolist()
    tools_list.sort()
    list_fig = []
    list_fig_dialog = []
    for tool in tools_list:
        df_temp = df[df['Tool'] == tool]
        fig = px.pie(df_temp.head(8), names="Subject", values="Total Courses", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Subject", values="Total Courses", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(tools_list))

def job_graph1(session, keyspace):
    query = f'select industry,total_job from {keyspace}.industry_job_salary;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Total Postings']
    df = df.loc[df['Industry'] != 'None']

    fig = px.pie(df.head(8), names="Industry", values="Total Postings",height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
    fig.update_traces(textposition='inside', textinfo='label + value')

    fig_dialog = px.pie(df, names="Industry", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
    return fig, fig_dialog

def job_graph2(session, keyspace):
    query = f'select industry,min_salary, max_salary from {keyspace}.industry_job_salary;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Min Salary', 'Max Salary']
    df = df.loc[df['Industry'] != 'None']
    fig = px.bar(df.head(8), x='Industry', y=['Min Salary', 'Max Salary'], barmode='group', height=410, width=375,color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig.update_layout(margin=dict(t=5, l=0), yaxis_title='Salary')
    fig.update_traces(texttemplate='%{y}', textposition='inside')

    fig_dialog = px.bar(df, x='Industry', y=['Min Salary', 'Max Salary'], barmode='group', height=650, width =1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig_dialog.update_layout(
        xaxis_title='Industry',
        yaxis_title='Salary'
    )
    fig_dialog.update_traces(texttemplate='%{y}', textposition='inside')
    return fig, fig_dialog

def job_graph3(session, keyspace):
    query = f'select industry,programming_language,total_job from {keyspace}.industry_language_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Programming Language', 'Total Postings']
    df = df.loc[df['Industry'] != 'None']
    industries_list = df.groupby('Industry')['Total Postings'].sum().nlargest(10).index.tolist()
    industries_list.sort()
    list_fig = []
    list_fig_dialog = []
    for industry in industries_list:
        df_language = df[df['Industry'] == industry]
        fig = px.pie(df_language.head(8), names="Programming Language", values="Total Postings", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_language, names="Programming Language", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(industries_list))

def job_graph4(session, keyspace):
    query = f'select industry,framework,total_job from {keyspace}.industry_framework_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Framework', 'Total Postings']
    df = df.loc[df['Industry'] != 'None']
    industries_list = df.groupby('Industry')['Total Postings'].sum().nlargest(10).index.tolist()
    industries_list.sort()
    list_fig = []
    list_fig_dialog = []
    for industry in industries_list:
        df_language = df[df['Industry'] == industry]
        fig = px.pie(df_language.head(8), names="Framework", values="Total Postings", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_language, names="Framework", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(industries_list))

def job_graph5(session, keyspace):
    query = f'select industry,tool,total_job from {keyspace}.industry_tool_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Tool', 'Total Postings']
    df = df.loc[df['Industry'] != 'None']
    industries_list = df.groupby('Industry')['Total Postings'].sum().nlargest(10).index.tolist()
    industries_list.sort()
    list_fig = []
    list_fig_dialog = []
    for industry in industries_list:
        df_language = df[df['Industry'] == industry]
        fig = px.pie(df_language.head(8), names="Tool", values="Total Postings", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_language, names="Tool", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(industries_list))

def job_graph6(session, keyspace):
    query = f'select tech_type,tech_name,total_job from {keyspace}.top_tech_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Technology Type', 'Technology Name','Total Postings']
    df = df.loc[df['Technology Name'] != 'None']
    df_limited = df.groupby('Technology Type').apply(lambda x: x.nlargest(10, 'Total Postings')).reset_index(drop=True)
    fig = px.bar(df_limited, x='Technology Type', y='Total Postings', color='Technology Name', barmode='group', height=410, width=375,color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig.update_layout( margin=dict(t=5, l=0))
    fig.update_traces(texttemplate='%{y}', textposition='inside')

    fig_dialog = px.bar(df_limited, x='Technology Type', y='Total Postings', color='Technology Name', barmode='group', height=650, width =1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
    fig_dialog.update_traces(texttemplate='%{y}', textposition='inside')
    fig_dialog.update_layout( xaxis_title='Technology Type', yaxis_title='Total Postings')
    return fig, fig_dialog

def job_graph7(session, keyspace):
    query = f'select industry,programming_language,total_job from {keyspace}.industry_language_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Programming Language', 'Total Postings']
    df = df.loc[df['Programming Language'] != 'None']
    languages_list = df.groupby('Programming Language')['Total Postings'].sum().nlargest(10).index.tolist()
    languages_list.sort()
    list_fig = []
    list_fig_dialog = []
    for language in languages_list:
        df_temp = df[df['Programming Language'] == language]
        fig = px.pie(df_temp.head(8), names="Industry", values="Total Postings", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Industry", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(languages_list))

def job_graph8(session, keyspace):
    query = f'select industry,framework,total_job from {keyspace}.industry_framework_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Framework', 'Total Postings']
    df = df.loc[df['Framework'] != 'None']
    frameworks_list = df.groupby('Framework')['Total Postings'].sum().nlargest(10).index.tolist()
    frameworks_list.sort()
    list_fig = []
    list_fig_dialog = []
    for framework in frameworks_list:
        df_temp = df[df['Framework'] == framework]
        fig = px.pie(df_temp.head(8), names="Industry", values="Total Postings", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Industry", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(frameworks_list))

def job_graph9(session, keyspace):
    query = f'select industry,tool,total_job from {keyspace}.industry_tool_job;'
    result = session.execute(query)
    df = pd.DataFrame(list(result))
    df.columns = ['Industry', 'Tool', 'Total Postings']
    df = df.loc[df['Tool'] != 'None']
    tools_list = df.groupby('Tool')['Total Postings'].sum().nlargest(10).index.tolist()
    tools_list.sort()
    list_fig = []
    list_fig_dialog = []
    for tool in tools_list:
        df_temp = df[df['Tool'] == tool]
        fig = px.pie(df_temp.head(8), names="Industry", values="Total Postings", height= 420, width= 420,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig.update_layout(showlegend=False, margin=dict(t=15, l=0))
        fig.update_traces(textposition='inside', textinfo='label + value')
        list_fig.append(fig)

        fig_dialog = px.pie(df_temp, names="Industry", values="Total Postings", height=750, width= 1000,color_discrete_sequence=px.colors.qualitative.Pastel2)
        fig_dialog.update_traces(textposition='inside', textinfo='label+percent+value')
        list_fig_dialog.append(fig_dialog)

    return list_fig ,list_fig_dialog, list(enumerate(tools_list))