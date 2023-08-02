import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

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
            name = soup.find("div", class_='col-md-7 pr-4').find('h1').text
            row.append(name)

            row.append(url)

            row.append("0")
            enrolls = soup.find("div",class_='small',attrs={"data-test-id": "selector-subheading"})
            if enrolls !=None:
                row.append(enrolls.find('div').text.split(' ')[0].replace(",", ""))
            else:
                row.append("0")

            instructor = soup.find("span", class_="font-weight-bold", text="Institution:")
            if instructor != None:
                next_a_element = instructor.find_next_sibling("a")
                row.append(next_a_element.text)
            else:
                row.append("")

            times = soup.find_all("div",class_ ='ml-3')
            string = times[0].text
            numbers = re.findall(r'\d+', string)
            row.append((int(numbers[2]) - int(numbers[1])) * int(numbers[0]))

            level = soup.find("span", class_="font-weight-bold", text="Level: ").parent
            if level:

                row.append(level.text.split(' ')[2])
            else:
                row.append("")
            row.append(0)
            
            programs = soup.find_all("a", class_="muted-link inline-link text-wrap")
            ins = ""
            if programs:

                for i in range(len(programs)):
                    span_tag = programs[i]
                    if span_tag.text !="":
                        ins = ins + span_tag.text
                        if i !=len(programs)-1:
                            ins=ins+", "
                row.append(ins)
            else:
                row.append("")

            subtitle = soup.find("span", class_="font-weight-bold", text="Language: ").parent
            if subtitle:

                row.append(subtitle.text.split(' ')[2])
            else:
                row.append("")

            subject = soup.find("span", class_="font-weight-bold", text="Subject: ")
            if subject != None:
                next_a_element = subject.find_next_sibling("a")
                row.append(next_a_element.text)
            else:
                row.append("")
            row.append("")
            
            skills = soup.find("span", class_="font-weight-bold", text="Associated skills: ")
            if skills != None:
                next_a_element = skills.find_next_sibling("span")
                row.append(next_a_element.text)
            else:
                row.append("")
        rows.append(row)

    edx = pd.DataFrame(rows,columns=['name', 'link','rating','enroll','instructor','time','level','fee','program','subtitle','subject','organization','skillsgain'])
    return edx