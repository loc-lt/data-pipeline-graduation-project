import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import subprocess

def crawl(list_urls):
        
    rows = []
    for attribute in list_urls.itertuples():
        url = 'https://www.udemy.com/'+ attribute.link
        response = requests.get(url)
        row = []
        # Kiểm tra mã trạng thái của yêu cầu
        if response.status_code == 200:
            # Sử dụng BeautifulSoup để phân tích cú pháp HTML
            soup = BeautifulSoup(response.content, "html.parser")
        name = soup.find("h1", class_='ud-heading-xl clp-lead__title clp-lead__title--small')
        if name ==None:
            continue
        rating = soup.find("span",class_ ='ud-heading-sm star-rating-module--rating-number--2xeHu')
        row.append(name.text)
        row.append(url)
        if rating !=None:
            row.append(rating.text)
        else:
            row.append("0")
        enrolls = soup.find("div",class_='enrollment')
        if enrolls !=None: 
            if ''.join(filter(str.isdigit, enrolls.text.split()[0].replace(",", ""))) == "":
                row.append("0")
            else:
                row.append(int())
        else:
            row.append("0")
        instructors = soup.find_all("a",class_='ud-btn ud-btn-large ud-btn-link ud-heading-md ud-text-sm ud-instructor-links')
        ins = ""
        instructor=""
        if instructors !=None:
            
            for i in range(len(instructors)):
                span_tag = instructors[i].find('span')
                if span_tag.text !="":
                    ins = ins + span_tag.text
                    if i !=len(instructors)-1:
                        ins=ins+", "
            row.append(ins)
        else:
            row.append(instructor)
        row.append(attribute.time)
        row.append(attribute.level)
        fees = soup.find("div",class_='price-text--price-part--2npPm ud-clp-discount-price ud-heading-xxl')
        if fees !=None:
            row.append(fees.text.replace(",", "").replace("₫", ""))
        else:
            row.append("0")
        program = ""
        row.append(program)

        subtitle = soup.find("div",class_='ud-text-sm caption--captions--joQAG')
        if subtitle !=None:
            string = subtitle.find('span').text
            #string = "English [Auto],\xa0French [Auto]"
            string = string.replace("[Auto]", "").replace("\xa0", "").strip().replace(" ", "")
            row.append(string)
        else:
            row.append("")

        subjects = soup.find_all("a",class_='ud-heading-sm')
        subject=""
        if subjects !=None or len(subjects) >= 2:
            
            row.append(subjects[1].text)
        else:
            row.append(subject)

        skillgains = soup.find("div",class_='ud-text-md clp-lead__headline')
        skillgain=""
        if skillgains !=None:
            row.append(skillgains.text)
        else:
            row.append(skillgain)
        rows.append(row)

    
    udemy = pd.DataFrame(rows,columns=['name', 'link','rating','enroll','instructor','time','level','fee','program','subtitle','subject','skillsgain'])
    return udemy