import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas as pd

def crawl(list_urls):
    rows = []
    for url in list_urls:
    # Gửi yêu cầu GET để lấy nội dung trang web
        response = requests.get(url)
        row = []
        # Kiểm tra mã trạng thái của yêu cầu
        if response.status_code == 200:
            # Sử dụng BeautifulSoup để phân tích cú pháp HTML
            soup = BeautifulSoup(response.content, "html.parser")
            subjects = soup.find_all("a",class_ = "_172v19u6 color-white font-weight-bold")
            if subjects == [] or len(subjects) == 1:
                continue
            subject = subjects[1].text.strip()
            if subject not in ["Computer Science","Data Science","Information Technology"]:
                
                continue
            
            name = soup.find("h1", class_='banner-title banner-title-without--subtitle m-b-0')
            row.append(name.text)
            row.append(url)

            rating = soup.find("span",class_ ='_16ni8zai m-b-0 rating-text number-rating m-l-1s m-r-1')
            if rating !=None:
                row.append(rating.text.split('s')[0])
            else:
                row.append("0")
            enrolls = soup.find("div",class_='_1fpiay2')
            if enrolls !=None:
                row.append(int(enrolls.text.split(' ')[0].replace(",", "")))
            else:
                row.append("0")
            instructors = soup.find_all("h3", class_="instructor-name headline-3-text bold")
            if instructors !=None:
                instructor=""
                for i in range(len(instructors)):
                    instructor = instructor+instructors[i].text
                    if i !=len(instructors)-1:
                        instructor=instructor+", "
                row.append(instructor)
            else:
                row.append("")

            times = soup.find_all("div", class_ = '_16ni8zai m-b-0 m-t-1s')
            
            flag2 = 0
            flag3 = 0
            if times !=None:
                for time in times:
                    if 'Approx.' in time.text:
                        
                        row.append(time.text.split(' ')[1] )
                        flag2 = 1  
                        break
                if flag2 == 0:
                    row.append("0")
            else:
                row.append("0")
            levels = soup.find_all("div",class_="_16ni8zai m-b-0")
            if levels !=None:
                for level in levels:
                    if level.text.find("Level") != -1:
                        row.append(level.text.split(' ')[0])
                        flag3 =1
                        break
                if flag3 == 0:
                    row.append("0")  
            else:
                row.append("0")
            
            prog = ""
            programs = soup.find("a",attrs={'data-click-key':"xdp_v1.xdp.click.banner_to_specialization"})
            if programs == None:
                row.append("")
            else:
                prog = prog+programs.text
                row.append(prog)
            subtitle = soup.find('div',class_='rc-TogglableContent collapsed')
            if subtitle !=None:
                row.append(subtitle.text[11:])
            else:
                row.append("")
            #subjects = soup.find_all("a",class_ = "_172v19u6 color-white font-weight-bold")

            #subject = subjects[1].text.strip()
            row.append(subject)

            organization = soup.find("img",class_='_1g3eaodg')
            if organization == None:
                organizations = soup.find_all("div",class_='m-b-1s m-r-1')
                if organizations != None:
                    org=""
                    for i in range(len(organizations)):
                        org = org + organizations[i].text
                        if i != len(organizations)-1:
                            org = org + ", "
                    row.append(org)
                else:
                    row.append("")
            else:
                row.append(organization['title'])

            skillgains = soup.find_all("span",class_="_rsc0bd m-r-1s m-b-1s")
            if skillgains !=None:
                skill =""
                for i in range(len(skillgains)):
                    skill= skill + skillgains[i].text
                    if i != len(skillgains) -1:
                        skill = skill + ','
                row.append(skill)
            else:
                row.append("")
        rows.append(row)

    coursera = pd.DataFrame(rows,columns=['name', 'link','rating','enroll','instructor','time','level','fee','program','subtitle','organization','skillsgain'])
    return coursera