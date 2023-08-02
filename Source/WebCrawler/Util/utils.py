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