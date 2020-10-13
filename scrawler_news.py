import requests , re , os , base64 , imghdr , shutil , bs4 , argparse , datetime , json , csv
import pytz as tz
import urllib3.request
from bs4 import BeautifulSoup
import json
import pandas as pd
#import psycopg2
import pymongo
import elasticsearch
from multiprocessing import Pool
import pymysql.cursors


date = datetime.datetime.now(tz.UTC)
today = date.today().strftime('%d/%m/%Y')
databases = ['mysql', 'mongo', 'elasticsearch','pgsql']
links = ['http://vneconomy.vn', 'https://baomoi.com', 'https://vnexpress.net', 'https://dantri.com.vn']
black_url_list = ['tim-kiem-tac-gia', 'javascript:', 'mailto:', '//']

try:

    if 'mysql' in databases:
    # Connect to the mysql database
        conn = pymysql.connect(host='127.0.0.1',
                                     port=3306,
                                     user='root',
                                     password='password',
                                     db='crawler',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        #create new table
        try:
            tablename = 'rawNews'
            query = f'''
            CREATE TABLE {tablename} (
                id int(11)  NOT NULL AUTO_INCREMENT,
                website_name varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                category varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                subcategory text COLLATE utf8_bin DEFAULT NULL,
                titles varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                discription text COLLATE utf8_bin DEFAULT NULL,
                author varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                original_images varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                downloaded_images varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                content_text_only text COLLATE utf8_bin DEFAULT NULL,
                content_part_objects text COLLATE utf8_bin DEFAULT NULL,
                likes varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                original_links varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                publishdate_at varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                downloaded_at varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                updated_at varchar(1024) COLLATE utf8_bin DEFAULT NULL,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
            AUTO_INCREMENT=1 ; '''
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
        except:
            pass

    if 'elasticsearch' in databases:
        pass

    if 'mongo' in databases:
        pass

    if 'pgsql' in databases:
        pass

except Exception as e:
    print('không kết nối được database')
    print(e)


class UltilCrawler:

    def __init__(self):
        pass

    def check_url_exits(self,url):

        is_check_ok = True

        try:
            with conn.cursor() as cursor:
                # SQL
                sql = f'''SELECT * FROM {tablename}'''
                # Thực thi câu lệnh truy vấn (Execute Query).
                cursor.execute(sql)
                # results = cursor.fetchone()  # nay se tra ve dang json
                results = cursor.fetchall()   # ket qua tra ve dang dict
                data = []
                if len(results) > 0:
                    for rows in results:
                        data.append(rows)

                    link_exits = [row['original_links'] for row in data]
                    if url in link_exits:
                        is_check_ok = False
            # print(is_check_ok)
            return is_check_ok

        except Exception as e:
            print(e)

    def all_dirs(self, path):
        paths = []
        for dir in os.listdir(path):
            if os.path.isdir(path + '/' + dir):
                paths.append(path + '/' + dir)

        return paths

    def all_files(self, path):
        paths = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if os.path.isfile(path + '/' + file):
                    paths.append(path + '/' + file)

        return paths


    def validate_image(self, path):
        ext = imghdr.what(path)
        if ext == 'jpeg':
            ext = 'jpg'
        return ext  # returns None if not valid

    def get_extension_from_link(self, link, default='jpg'):
        splits = str(link).split('.')
        if len(splits) == 0:
            return default
        ext = splits[-1].lower()
        if ext == 'jpg' or ext == 'jpeg':
            return 'jpg'
        elif ext == 'gif':
            return 'gif'
        elif ext == 'png':
            return 'png'
        elif ext == 'svg':
            return 'svg'
        else:
            return default

    def make_dir(self, dirname):
        current_path = os.getcwd()
        path = os.path.join(current_path, dirname)
        if not os.path.exists(path):
            os.makedirs(path)

    def save_object_to_file(self, object, file_path, is_base64=False):
        try:
            with open('{}'.format(file_path), 'wb') as file:
                if is_base64:
                    file.write(object)
                else:
                    shutil.copyfileobj(object.raw, file)
        except Exception as e:
            print('Save failed - {}'.format(e))

    def base64_to_object(self, src):
        header, encoded = str(src).split(',', 1)
        data = base64.decodebytes(bytes(encoded, encoding='utf-8'))
        return data


    # def download_images(self, keyword='', links=[''], site_name='', max_count=0):
    def download_images(self, keyword='Tai-Chinh', site_name='', max_count=0):
        links = ['https://vneconomy.mediacdn.vn/zoom/700_400/2019/11/1/ngoai-te-1572594541586693823234-crop-15792434472561961367127.jpg',
                 'https://vneconomy.mediacdn.vn/zoom/700_400/2020/1/15/mbbank-15790813670371719997886-crop-1579081371647469137936.png',
                 'https://vnreview.vn/image/16/81/51/1681516.jpg?t=1499848476274']
        download_path = 'Download-Images'
        self.make_dir('{}/{}'.format(download_path, keyword.replace('"', '')))
        total = len(links)
        success_count = 0

        if max_count == 0:
            max_count = total

        for index, link in enumerate(links):
            if success_count >= max_count:
                break

            try:
                print('Downloading {} from {}: {} / {}'.format(keyword, site_name, success_count + 1, max_count))

                if str(link).startswith('data:image/jpeg;base64'):
                    response = self.base64_to_object(link)
                    ext = 'jpg'
                    is_base64 = True
                elif str(link).startswith('data:image/png;base64'):
                    response = self.base64_to_object(link)
                    ext = 'png'
                    is_base64 = True
                else:
                    response = requests.get(link, stream=True)
                    ext = self.get_extension_from_link(link)
                    is_base64 = False

                no_ext_path = '{}/{}/{}_{}'.format(download_path.replace('"', ''), keyword, site_name,
                                                   str(index).zfill(4))
                path = no_ext_path + '.' + ext
                self.save_object_to_file(response, path, is_base64=is_base64)

                success_count += 1
                del response

                ext2 = self.validate_image(path)
                if ext2 is None:
                    print('Unreadable file - {}'.format(link))
                    os.remove(path)
                    success_count -= 1
                else:
                    if ext != ext2:
                        path2 = no_ext_path + '.' + ext2
                        os.rename(path, path2)
                        print('Renamed extension {} -> {}'.format(ext, ext2))

            except Exception as e:
                print('Download failed - ', e)
                continue


class VneconomyScrawl:

    def __init__(self, urls):
        self.url = urls

    def write_csv_file(self, csv_file_path, headers_to_write, data_to_write):
        """
        :param csv_file_path: pass in full_file_path var pointing to desired csv output
        :param headers_to_write: pass in list of headers
        :param data_to_write: pass in list of data
        :return:
        """
        csv.register_dialect('excel', delimiter=',', quoting=csv.QUOTE_ALL)

        # grab dict keys from data_to_write var as row iterator
        # row_iterator = list(data_to_write)

        with open(csv_file_path, 'wt') as f:
            try:
                writer = csv.writer(f, dialect='excel')

                # write headers
                writer.writerow(headers_to_write)

                # write data
                # for row in row_iterator:
                writer.writerow(data_to_write)
            finally:
                print('CSV file written successfully.')
                print('\n')
                f.close()


    def test_get_data(self):
        try:
            with conn.cursor() as cursor:
                # SQL
                # sql  = f'''SELECT * FROM {tablename} ORDER BY id DESC LIMIT 2'''
                sql  = f'''SELECT * FROM {tablename}'''
                # Thực thi câu lệnh truy vấn (Execute Query).
                cursor.execute(sql)
                data = cursor.fetchall()
                data_json = json.dumps(data)
                fieldnames = ['id', 'website_name', 'category', 'subcategory', 'titles', 'discription', 'author',
                              'original_images', 'downloaded_images', 'content_text_only', 'content_part_objects',
                              'likes', 'original_links', 'publishdate_at', 'downloaded_at', 'updated_at'
                              ]
                list2 = []
                for row in data:
                    print(row)
                    list2.append([row['id'],row['website_name'],row['category'],row['subcategory'],row['titles'],row['discription'],row['author'],row['original_images'],row['downloaded_images'],row['content_text_only'],row['content_part_objects'],row['likes'],row['original_links'],row['publishdate_at'],row['publishdate_at'],row['downloaded_at'],row['updated_at']])

                    self.write_csv_file('data.csv', fieldnames, list2)

                with open('data.json', 'w+') as f:
                    f.write(str(data_json))

        finally:
            pass
            # Đóng kết nối (Close connection).
            conn.close()


    def load_website(self,url,prefix):
        try:
            response = requests.get(prefix+url)
            return BeautifulSoup(response.content,'html.parser')
            # return BeautifulSoup(response.text)
        except Exception as err:
            print(f'ERROR: {err}')


    def scraping_and_insert_database(self,articles, cartgory_text, tablename, link):

            try:
                with conn.cursor() as cursor:

                    if articles[0].img['src'] is not None:
                        original_images = articles[0].img['src']
                    else:
                        original_images = ''

                    if articles[0].find('h2', {'class': 'sapo'}) is not None:
                        discription = articles[0].find('h2', {'class': 'sapo'}).get_text()
                    else:
                        discription = ''

                    if articles[0].find('p', {'class': 'name'}) is not None:
                        author = articles[0].find('p', {'class': 'name'}).get_text()
                    else:
                        author = ''

                    if articles[0].find('p', {'class': 'time'}) is not None:
                        publishdate_at = articles[0].find('p', {'class': 'time'}).get_text()
                    else:
                        publishdate_at = ''

                    category = cartgory_text
                    likes = articles[0].find_all('span', {'id': 'u_0_3'}, {'class': '_5n6h _2pih'})
                    titles = articles[0].find('h1',{'class':'title'}).get_text()
                    subcategory = ''
                    content_text_only = articles[0].find('div',{'class':'contentdetail'},{'data-role':'content'}).text.strip().replace('\'', '').replace('"', '')
                    content_part_objects = ''
                    website_name = self.url
                    downloaded_images = ''
                    original_links = link
                    downloaded_at = today
                    updated_at = today

                    check = UltilCrawler()
                    check_urls = check.check_url_exits(link)

                    # if check_urls is True:
                        # build query string
                    query = f"""INSERT INTO {tablename}(
                                                original_images, category, subcategory, titles, 
                                                discription, author, publishdate_at, likes,
                                                content_text_only, content_part_objects, original_links, 
                                                website_name, downloaded_images, downloaded_at, updated_at
                                            )
                        VALUES(
                            '{original_images}', '{category}', '{subcategory}', '{titles}', '{discription}',
                            '{author}', '{publishdate_at}', '{likes}', '{content_text_only}', 
                            '{content_part_objects}', '{original_links}', '{website_name}', '{downloaded_images}',
                            '{downloaded_at}','{updated_at}'
                        );"""
                    ## commit to connection
                    cursor.execute(query)
                    conn.commit()
                    print('insert done')

                    # elif check_urls is False:

                    # query = f'''update  {tablename} set original_images='{original_images}',subcategory='{subcategory}',
                    #         titles='{titles}',discription='{discription}', author='{author}', likes='{likes}',
                    #         content_text_only='{content_text_only}', content_part_objects='{content_part_objects}',
                    #         downloaded_images='{downloaded_images}', updated_at='{updated_at}';'''
                    #
                    # ## commit to connection
                    # cursor.execute(query)
                    # conn.commit()
                    # print('update done')
            # except:
            #     pass
            except Exception as err:
                print(err)


    def scrawling(self):
        #load trang home page wedsite can scrawl
        soup = self.load_website(self.url,prefix='')
        #scrape the categories and their links and store in array
        nav = soup.ul.find_all('a', limit=None)
        category, link = [], []
        for h in range(len(nav)):
            try:
                if nav[h].text != '\n':
                    link.append(nav[h]['href'])
                    category.append(nav[h].text)
            except:
                print('pass')
        category_nav = list(zip(category,link))

        for j in range(2,len(category_nav) - 6):
            try:
                soup = self.load_website(category_nav[j][1], prefix=self.url)
                tag = soup.find_all('a', limit=None)
                cartgory_text = category_nav[j][0]
                category_item, link_item = [], []

                for h in range(len(tag)):
                    try:
                        if '\n' not in tag[h].text and tag[h]['href'] not in link and 'tim-kiem-tac-gia' not in tag[h]['href'] and 'javascript:' not in tag[h]['href'] and 'mailto:' not in tag[h]['href'] and '//' not in tag[h]['href']:
                        # if '\n' not in tag[h].text and tag[h]['href'] not in link:
                            link_item.append(tag[h]['href'])
                            category_item.append(tag[h].text)

                    except Exception as err:
                        print(err)

                categories = list(zip(category_item, link_item))

                for k in range(0,len(link_item)):
                    ks = categories[k][1]
                    soup_item = self.load_website(categories[k][1], prefix=self.url)
                    articles = soup_item.find_all('div',{'class':'contentleft fl'}, limit=None)
                    check_text = soup_item.find('b').get_text()
                    if len(articles) != 0 and check_text != '':
                        prefix = self.url
                        link = str(prefix + '{}'.format(link_item[k]))
                        self.scraping_and_insert_database(articles, cartgory_text, tablename, link)

                # Read next page cursor at the bottom of a product page
                # links = soup.find_all('div', {"class": 'list-pager'})
                #
                # # While next page cursor is not empty, read next page cursor to move to next product page
                # while links[0].find_all('a', {"class": "next"}) != []:
                #     try:
                #         soup = load_website(links[0].find_all('a', {"class": "next"})[0]['href'], prefix='http://vneconomy.vn')
                #         articles = soup.find_all('div', {"class": "product-item"})
                #         print('Reading', cat[j][0], links[0].find_all('a', {"class": "next"})[0]['href'].split('&')[1], sep=' ')
                #         for i in range(len(articles)):
                #             scrape_and_insert(cat, j, articles, i, cur, conn, tablename)
                #         links = soup.find_all('div', {"class": 'list-pager'})
                #     except:
                #         continue

            except:
                continue
            print(f''' Scrawled {self.url} success! ''')


class VnexpressScrawl:

    def __init__(self, urls):
        self.url = urls


    def write_csv_file(self, csv_file_path, headers_to_write, data_to_write):
        """
        :param csv_file_path: pass in full_file_path var pointing to desired csv output
        :param headers_to_write: pass in list of headers
        :param data_to_write: pass in list of data
        :return:
        """
        csv.register_dialect('excel', delimiter=',', quoting=csv.QUOTE_ALL)

        # grab dict keys from data_to_write var as row iterator
        # row_iterator = list(data_to_write)

        with open(csv_file_path, 'wt') as f:
            try:
                writer = csv.writer(f, dialect='excel')

                # write headers
                writer.writerow(headers_to_write)

                # write data
                # for row in row_iterator:
                writer.writerow(data_to_write)
            finally:
                print('CSV file written successfully.')
                print('\n')
                f.close()

    def test_get_data(self):
        try:
            with conn.cursor() as cursor:
                # SQL
                # sql  = f'''SELECT * FROM {tablename} ORDER BY id DESC LIMIT 2'''
                sql = f'''SELECT * FROM {tablename}'''
                # Thực thi câu lệnh truy vấn (Execute Query).
                cursor.execute(sql)
                data = cursor.fetchall()
                data_json = json.dumps(data)
                fieldnames = ['id', 'website_name', 'category', 'subcategory', 'titles', 'discription', 'author',
                              'original_images', 'downloaded_images', 'content_text_only', 'content_part_objects',
                              'likes', 'original_links', 'publishdate_at', 'downloaded_at', 'updated_at'
                              ]
                list2 = []
                for row in data:
                    print(row)
                    list2.append([row['id'], row['website_name'], row['category'], row['subcategory'], row['titles'],
                                  row['discription'], row['author'], row['original_images'], row['downloaded_images'],
                                  row['content_text_only'], row['content_part_objects'], row['likes'],
                                  row['original_links'], row['publishdate_at'], row['publishdate_at'],
                                  row['downloaded_at'], row['updated_at']])

                    self.write_csv_file('data.csv', fieldnames, list2)

                with open('data.json', 'w+') as f:
                    f.write(str(data_json))

        finally:
            pass
            # Đóng kết nối (Close connection).
            conn.close()

    def load_website(self, url, prefix):
        try:
            response = requests.get(prefix + url)
            return BeautifulSoup(response.content, 'html.parser')
            # return BeautifulSoup(response.text)
        except Exception as err:
            print(f'ERROR: {err}')

    def scraping_and_insert_database(self, articles, cartgory_text, tablename, link):

        try:
            with conn.cursor() as cursor:

                if articles[0].meta['content'] is not None:
                    original_images = articles[0].meta['content']
                else:
                    original_images = ''

                if articles[0].find('p', {'class': 'description'}) is not None:
                    discription = articles[0].find('p', {'class': 'description'}).get_text()
                else:
                    discription = ''

                if articles[0].find('strong') is not None:
                    author = articles[0].find('strong').get_text()
                else:
                    author = ''

                if articles[0].find('span', {'class': 'date'}) is not None:
                    publishdate_at = articles[0].find('span', {'class': 'date'}).get_text()
                else:
                    publishdate_at = ''

                category = cartgory_text
                likes = articles[0].find_all('span', {'id': 'u_0_3'}, {'class': '_5n6h _2pih'})
                titles = articles[0].find('h1', {'class': 'title-detail'}).get_text()
                subcategory = ''
                tag_p = articles[0].find_all('p', {'class': 'Normal'})
                content_text_list = []
                for p in tag_p:
                    p = p.text.strip().replace('\'', '').replace('"', '')
                    content_text_list.append(p)

                # content_text_only = '.'.join(map(str, content_text_list))
                content_text_only = '.'.join(content_text_list)

                content_part_objects = content_text_list
                website_name = self.url
                downloaded_images = ''
                original_links = link
                downloaded_at = today
                updated_at = today

                # check_urls = self.Check.check_url_exits(link)
                #
                # if check_urls is True:
                # build query string
                query = f"""INSERT INTO {tablename}(
                                                original_images, category, subcategory, titles,
                                                discription, author, publishdate_at, likes,
                                                content_text_only, content_part_objects, original_links,
                                                website_name, downloaded_images, downloaded_at, updated_at
                                            )
                        VALUES(
                            '{original_images}', '{category}', '{subcategory}', '{titles}', '{discription}',
                            '{author}', '{publishdate_at}', '{likes}', '{content_text_only}',
                            '{content_part_objects}', '{original_links}', '{website_name}', '{downloaded_images}',
                            '{downloaded_at}','{updated_at}'
                        );"""
                # commit to connection
                cursor.execute(query)
                conn.commit()
                print('insert done')

                # elif check_urls is False:
                #
                #     query = f'''update  {tablename} set original_images='{original_images}',subcategory='{subcategory}',
                #                 titles='{titles}',discription='{discription}', author='{author}', likes='{likes}',
                #                 content_text_only='{content_text_only}', content_part_objects='{content_part_objects}',
                #                 downloaded_images='{downloaded_images}', updated_at='{updated_at}';'''
                #
                #     # commit to connection
                #     cursor.execute(query)
                #     conn.commit()
                #     print('update done')
        except:
            pass
        # except Exception as err:
        #     print(err)

    def scrawling(self):
        # load trang home page wedsite can scrawl
        soup = self.load_website(self.url, prefix='')
        # scrape the categories and their links and store in array
        nav = soup.ul.find_all('a', limit=None)
        link = []
        category = []
        for a in nav:
            a = a['href'].replace('\n','')
            link.append(a)
        for h in range(len(nav)):
            try:
                    b = nav[h].text.replace('\n', '')
                    category.append(b)
            except:
                print('pass')

        category_nav = list(zip(category, link))

        for j in range(6,len(category_nav)):
            try:
                if 'kinh-doanh' in category_nav[j][1]:
                    sublinks = ['chung-khoan', 'ebank']

                    for sublink in sublinks:
                        categ = ''
                        category_item, link_item = [], []

                        if 'chung-khoan' == sublink:
                            categ.replace('','CHỨNG KHOÁN')

                        if 'ebank' == sublink:
                            categ.replace('','TÀI CHÍNH')

                        soup = self.load_website(category_nav[j][1] + '/' + sublink, prefix=self.url)
                        resp = soup.find('div',{'class':'container flexbox'}).find_all('a')
                        for row in resp:
                            link_item.append(row['href'])

                            hh = row.text
                            if '\n' in hh:
                                res = hh.replace('\n', '')
                                category_item.append(res)

                        categories = list(zip(category_item, link_item))

                        for k in range(len(link_item)):
                            ks = categories[k][1]
                            soup_item = self.load_website(categories[k][1], prefix='')
                            articles = soup_item.find('div', {'class': 'sidebar-1'})
                            # check_text = soup_item.find('b').get_text()
                            # if len(articles) != 0:
                            link_add = ks
                            self.scraping_and_insert_database(articles, categ, tablename, link_add)

                #Read next page cursor at the bottom of a product page
                # links = soup.find_all('div', {"class": 'list-pager'})
                #
                # # While next page cursor is not empty, read next page cursor to move to next product page
                # while links[0].find_all('a', {"class": "next"}) != []:
                #     try:
                #         soup = self.load_website(links[0].find_all('a', {"class": "next"})[0]['href'], prefix=self.url)
                #         articles = soup.find_all('div', {"class": "product-item"})
                #         print('Reading', category_nav[j][0], links[0].find_all('a', {"class": "next"})[0]['href'].split('&')[1], sep=' ')
                #         for i in range(len(articles)):
                #             self.scraping_and_insert_database(category_nav, j, articles, i, tablename)
                #         links = soup.find_all('div', {"class": 'list-pager'})
                #     except:
                #         continue

            except:
                pass
            print(f''' Scrawled {self.url} success! ''')


class BaomoiScrawl:

    def __init__(self):
        pass

    pass


class DantriScrawl:

    def __init__(self):
        pass

    pass

def main():

    for link in links:
        if 'http://vneconomy.vn' == link:
            vneconomy = VneconomyScrawl(link)
            vneconomy.scrawling()
            test = vneconomy.test_get_data()
            print(test)
            print('==== vneconomy done ====')

        # if 'https://vnexpress.net' == link:
        #     vnexpress = VnexpressScrawl(link)
        #     vnexpress.scrawling()
        #     test = vnexpress.test_get_data()
        #     print(test)
        #     print('==== vnexpress done ====')

        if 'https://baomoi.com' == link:
            pass

        if 'https://dantri.com.vn' == link:
            pass


main()
# a = UltilCrawler()
# a.check_url_exits('http://vneconomy.vn/chung-khoan-sang-15-1-bid-xuat-sac-ros-lai-mat-hut-lenh-khung-20200115120012346.htm')
