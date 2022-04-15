import copy
from flask import Flask, send_file, request, render_template, redirect, url_for
import requests
from bs4 import BeautifulSoup
import lxml
from collections import OrderedDict
import concurrent.futures
import asyncio
import csv
import datetime
import time
import pprint

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = app.root_path
st = time.perf_counter()


@app.route('/', methods=['GET', 'POST'])
def scraper():
    if request.method == 'POST':
        url = request.form.get("url")
        if 'https://www.4icu.org/' not in url:
            return render_template("base.html", item='Invalid URL')

        schema = OrderedDict(
            {'University Name': '', 'University Regional Name': '', 'Acronym': '', 'Founded': '', 'Country Rank': '',
             'World Rank': '', 'Address': '', 'Tel': '', 'Fax': '', 'Diploma': '', 'Bachelor': '', 'Master': '',
             'Doctorate': '', 'UG Tuition Fees - Local Students': '', 'UG Tuition Fees - International Students': '',
             'PG Tuition Fees - Local Students': '', 'PG Tuition Fees - International Students': '', 'Gender': '',
             'International Students': '', 'Selection Type': '', 'Admission Requirements': '',
             'Admission Rate': '', 'Admission Office': '', 'Minority Serving': '',
             'Student Enrollment': '', 'Academic Staff': '', 'Control Type': '', 'Entity Type': '',
             'Academic Calendar': '',
             'Campus Setting': '', 'Land Grant Institution': '', 'Religious Affiliation': '',
             'Basic Classification': '', '2000 Classification': '', 'Size & Setting': '', 'Enrollment Profile': '',
             'Undergraduate Profile': '',
             'Undergraduate Instructional Program': '', 'Graduate Instructional Program': '',
             'Library': '', 'Housing': '', 'Sport Facilities': '', 'Financial Aids': '', 'Academic Counseling': '',
             'Distance Learning': '', 'Study Abroad': '',
             'Career Services': '', 'Institutional Hospital': '', 'Institutional Accreditation': '',
             'Year of first Accreditation': '',
             'Memberships and Affiliations': '', 'Academic Structure': '', 'Facebook': '',
             'Twitter': '', 'LinkedIn': '', 'YouTube': '', 'Instagram': '', 'iTunes U': '', 'Open Education Global': '',
             'Overview': '', 'Wikipedia': ''})

        filename = []
        combined_final = []
        univ_list = []

        with open('draft.csv', 'w', newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=schema.keys(), extrasaction='ignore')
            writer.writeheader()

            path = f"{app.root_path}/draft.csv"

            def univ_detailed_scraper(link):
                response = requests.get(link)
                soup = BeautifulSoup(response.content, 'lxml')

                final = copy.deepcopy(schema)

                titles = {'University Overview': '', 'University Identity': '', 'University Location': '',
                          'Size and Profile': '',
                          'Study Areas and Degree Levels': '', 'Yearly Tuition Range': '', 'University Admissions': '',
                          'Facilities and Services': '', 'Accreditations': '', 'Memberships and Affiliations': '',
                          'Academic Structure': '', 'Social Media': '', 'Carnegie Classification': '',
                          'Wikipedia Article': '',
                          'Online Courses': '', 'Ranking': ''}

                def worker(data):

                    if data[0] == 'Ranking':
                        op = iter(
                            [j.text.strip().title() for i in soup.find_all('tr', style="vertical-align:bottom") if i for j
                             in
                             i.find_all('td') if j])
                        final.update(dict(zip(op, op)))

                    elif data[0] == 'University Overview' and data[1]:
                        overview = data[1].find('p', itemprop="description")
                        if overview:
                            final['Overview'] = overview.text.strip()

                    elif data[0] == 'University Identity' and data[1]:
                        tempui = {'University Name': data[1].find('span', itemprop="name").find('strong'),
                                  'University Regional Name': data[1].find('span', itemprop="alternateName"),
                                  'Acronym': data[1].find('abbr'),
                                  'Founded': data[1].find('span', itemprop="foundingDate")}

                        [final.update({i: j.text.strip()}) for i, j in tempui.items() if j]
                    #
                    # elif data[0] == 'University Location' and data[1]:
                    #     tempul = {'Address': data[1].find('td'),
                    #               'Tel': data[1].find('span', itemprop="telephone"),
                    #               'Fax': data[1].find('span', itemprop="faxNumber")}
                    #     [final.update({i: j.text.strip()}) for i, j in tempul.items() if j]

                    elif data[0] == 'Study Areas and Degree Levels' and data[1]:
                        op = data[1].find_all('tbody')
                        res = {'Diploma': [], 'Bachelor': [], 'Master': [], 'Doctorate': []}
                        for i in op:
                            for j in (i.find_all('tr')):
                                ip = j.find_all('td')
                                branch = ip[0].find('div', class_="hidden-xs").text.replace('\n', '').strip()

                                di = {'Diploma': ip[1].find('i')['class'], 'Bachelor': ip[2].find('i')['class'],
                                      'Master': ip[3].find('i')['class'], 'Doctorate': ip[4].find('i')['class']}

                                list(
                                    map(lambda x: res[x[0]].append(branch) if x[1] and x[1][1] == 'd1' else '', di.items()))

                        list(map(lambda x: final.update({x[0]: ', '.join(x[1])}), res.items()))

                    elif data[0] == 'Yearly Tuition Range' and data[1]:
                        op = [i.text.strip() for i in data[1].find_all('tr' and 'strong') if i]
                        op.pop()
                        if len(op) == 4:
                            final.update({'UG Tuition Fees - Local Students': op[0],
                                          'UG Tuition Fees - International Students': op[1],
                                          'PG Tuition Fees - Local Students': op[2],
                                          'PG Tuition Fees - International Students': op[3]})

                    elif data[1] and data[0] in ('University Location', 'University Admissions', 'Size and Profile',
                                                 'Facilities and Services', 'Carnegie Classification'):
                        final.update(dict(map(lambda x: (
                            x[0].text.strip().replace('New ', '').replace('Name', 'University Name'), x[1].text.strip()) if
                        x[0] and x[1] is not None else '',
                                              zip(data[1].find_all('th'), data[1].find_all('td')))))

                    # elif data[0] == 'Size and Profile' and data[1]:
                    #     final.update(dict(map(lambda x: (x[0].text.strip(), x[1].text.strip()) if x[0] and x[1] is not None else '',
                    #                           zip(data[1].find_all('th'), data[1].find_all('td')))))
                    #
                    # elif data[0] == 'Facilities and Services' and data[1]:
                    #     final.update(dict(map(lambda x: (x[0].text.strip(), x[1].text.strip()) if x[0] and x[1] is not None else '',
                    #                           zip(data[1].find_all('th'), data[1].find_all('td')))))
                    #
                    # elif data[0] == 'Carnegie Classification' and data[1]:
                    #     final.update(dict(map(lambda x: (x[0].text.strip(), x[1].text.strip()) if x[0] and x[1] is not None else '',
                    #                           zip(data[1].find_all('th'), data[1].find_all('td')))))

                    elif data[0] == 'Accreditations' and data[1]:
                        op = data[1].find_all('p')
                        if len(op) > 2 and op[2].text.strip() == '' and (op[0] and op[1] is not None):
                            final.update({'Institutional Accreditation': op[0].text.strip(),
                                          'Year of first Accreditation': op[1].text.strip()})
                        else:
                            url = data[1].find('a', href=True)
                            if url:
                                final.update({'Institutional Accreditation': url.text.strip()})

                    elif data[0] == 'Memberships and Affiliations' and data[1]:
                        op = [i.text.strip() for i in data[1].find_all('li') if i]
                        if op:
                            final.update({'Memberships and Affiliations': ','.join(op)})

                    elif data[0] == 'Academic Structure' and data[1]:
                        op = [i.find('strong').text.strip() for i in data[1].find_all('button') if i]
                        if op:
                            op.pop(0)
                            final.update({'Academic Structure': ', '.join(op)})

                    elif data[0] == 'Social Media' and data[1]:
                        op = [i['href'] for i in data[1].find_all('a', href=True) if 'http' in i['href']]
                        sm = {'Facebook': '', 'Twitter': '', 'LinkedIn': '', 'YouTube': '', 'Instagram': ''}
                        final.update({i: j for i in sm.keys() for j in op if i.lower() in j})

                    elif data[0] == 'Online Courses' and data[1]:
                        op = [i['href'] for i in data[1].find_all('a', href=True) if 'http' in i['href']]
                        sm = {'iTunes U': '', 'Open Education Global': ''}
                        for i in op:
                            if 'itunes' in i:
                                sm['iTunes U'] = sm['iTunes U'] + i + ' '
                            else:
                                sm['Open Education Global'] = sm['Open Education Global'] + i
                        final.update(sm)

                    elif data[0] == 'Wikipedia Article' and data[1]:
                        op = data[1].find('a', href=True)
                        if op:
                            final.update({'Wikipedia': op['href']})

                for i in soup.find_all('div', class_="panel panel-default"):
                    try:
                        temp_title = i.find('div', class_="panel-heading").text.replace('\n', '').replace(' New',
                                                                                                          '').strip()

                        if temp_title in titles:
                            titles[temp_title] = i
                    except:
                        pass

                [worker(i) for i in titles.items()]
                combined_final.append(final)

            def continent_scraper(link):
                response = requests.get(link)
                soup = BeautifulSoup(response.content, 'lxml')
                counties = []

                def dynamiclink(name):
                    data = soup.find_all('div', class_=name)
                    # table = soup.find_all('li', class_="list-group-item")

                    for i in data:
                        # for j in i.find_all('li', class_="list-group-item"):
                        for k in i.find_all('a', href=True):
                            counties.append(f"https://www.4icu.org{k['href']}")

                names = ("col-xs-offset-3 col-sm-4 col-sm-offset-2 col-md-4 col-md-offset-2 col-lg-3 col-lg-offset-3",
                         "col-xs-offset-3 col-sm-5 col-sm-offset-1", "col-xs-offset-4")
                list(map(dynamiclink, names))

                if counties:
                    with concurrent.futures.ThreadPoolExecutor() as ex:
                        o = ex.map(country_scraper, counties)
                else:
                    country_scraper(link)

            def country_scraper(link):
                response = requests.get(link)
                soup = BeautifulSoup(response.content, 'lxml')
                filename.append(
                    soup.find('h1').text.replace('Universities in ', '').replace('the ', '').replace('Top ', '').strip())
                table = soup.find_all('td', valign="top")
                states = []
                for i in table:
                    for j in i.find_all('a', href=True):
                        states.append(f"https://www.4icu.org{j['href']}")
                # states = [f"https://www.4icu.org{j['href']}" for k in soup.find_all('td', valign="top") for i in k for j in i.find_all('a', href=True)]
                if states:
                    with concurrent.futures.ThreadPoolExecutor() as ex:
                        o = ex.map(list_scraper, states)
                else:
                    list_scraper(link)

            def list_scraper(link):
                response = requests.get(link)
                soup = BeautifulSoup(response.content, 'lxml')
                table = soup.find_all('table', class_="table table-hover")
                for i in table:
                    for j in i.find_all('td' and 'a', href=True):
                        if j and 'reviews' in j['href']:
                            univ_list.append(f"https://www.4icu.org{j['href']}")

            # for i in list(listscraper()):
            #     univ_detailed_scraper(i)
            # with concurrent.futures.ThreadPoolExecutor() as ex:
            #     op = ex.map(univ_detailed_scraper, list(list_scraper()))
            try:
                continent_scraper(url)
            except Exception as e:
                print(e)
                pass

            with concurrent.futures.ThreadPoolExecutor() as ex:
                op = ex.map(univ_detailed_scraper, univ_list)

            if combined_final:
                writer.writerows(combined_final)

                over = round(time.perf_counter() - st, 2), 'Sec'
                print(over)

                return send_file(path,
                                 as_attachment=True,
                                 attachment_filename=f"{''.join(filename)} Universities.csv",
                                 mimetype='text/csv'
                                 )
        return render_template("base.html", item='No data found')
    else:
        return render_template("base.html", item="")


if __name__ == '__main__':
    app.run(debug=True)
