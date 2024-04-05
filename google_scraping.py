import threading
from selenium import webdriver
import time
import scrapy
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import psycopg2
import urllib.parse
from webdriver_manager.chrome import ChromeDriverManager
import requests
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime,timedelta
from psycopg2 import sql as sqlpsycop
import sys
source_data = "companies.csv"

data_table_updated = 'google_search_updated'
data_table_history = 'google_search_history'

def connection_db():
    # Database connection parameters
    dbname = "postgres"
    user = "dbuser"
    password = "black44#!55"
    host = "10.10.1.55"  # or your database server address
    port = "5432"  # or your database server port

    # Establish a connection to the database
    connection = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = connection.cursor()

    return [connection, cursor]

returntype = connection_db()
db = returntype[0]
cursor = returntype[1]



df = pd.read_csv('companies.csv')
df_dicts = df.to_dict('records')



def create_table_andinsert_data():
    # Check if the table exists
    check_table_query = sqlpsycop.SQL("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = {}
            )
        """).format(sqlpsycop.Literal(data_table_updated))
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]
    if not table_exists:
        # Define the SQL command to create the table for updated data
        create_table_query = sqlpsycop.SQL("""
                  CREATE TABLE IF NOT EXISTS {} (
                  id SERIAL PRIMARY KEY,
                  start_time TIMESTAMP DEFAULT NULL,
                  end_time TIMESTAMP DEFAULT NULL,
                  name TEXT,
                  company_id INT,
                  industry TEXT,
                  url TEXT UNIQUE,
                  website TEXT,
                  rating TEXT,
                  address TEXT,
                  phone TEXT,
                  review_count TEXT,
                  about TEXT,
                  linkedin TEXT,
                  youtube TEXT,
                  twitter TEXT,
                  facebook TEXT,
                  xing TEXT,
                  northdata TEXT,
                  creditreform TEXT,
                  side_bar_html TEXT,
                  search_results TEXT
                   );
            """).format(sqlpsycop.Identifier(data_table_updated))

        # Execute the SQL command
        cursor.execute(create_table_query)

        # Commit the changes to the database
        db.commit()

        print('--------------- PLease Wait Data is inserting from csv into the table (because table is creating for the first time) ---------------')

        for insertion in df_dicts:
            # Prepare SQL statement for inserting data
            sql = "INSERT INTO {} (name, company_id) VALUES (%s, %s)".format(data_table_updated)

            # Extract values from dictionaries and insert into database
            # Execute the insert operation in one go
            cursor.execute(sql, (insertion['name'], insertion['id']))
            # Commit the transaction
            db.commit()
            print('wait --- inserting')

        # Define the SQL command to create the table linkedin history
        create_table_query = sqlpsycop.SQL("""
                                CREATE TABLE IF NOT EXISTS {} (
                                  id SERIAL PRIMARY KEY,
                                  start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                  end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                  name TEXT,
                                  company_id INT,
                                  industry TEXT,
                                  url TEXT,
                                  website TEXT,
                                  rating TEXT,
                                  address TEXT,
                                  phone TEXT,
                                  review_count TEXT,
                                  about TEXT,
                                  linkedin TEXT,
                                  youtube TEXT,
                                  twitter TEXT,
                                  facebook TEXT,
                                  xing TEXT,
                                  northdata TEXT,
                                  creditreform TEXT,
                                  side_bar_html TEXT,
                                  search_results TEXT
                                    );
                            """).format(sqlpsycop.Identifier(data_table_history))

        # Execute the SQL command
        cursor.execute(create_table_query)

        # Commit the changes to the database
        db.commit()

    else:
        check_table_query = sqlpsycop.SQL("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = {}
                    )
                """).format(sqlpsycop.Literal(data_table_history))
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            create_table_query = sqlpsycop.SQL("""
                                            CREATE TABLE IF NOT EXISTS {} (
                                              id SERIAL PRIMARY KEY,
                                              start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                              end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                              name TEXT,
                                              company_id INT,
                                              industry TEXT,
                                              url TEXT,
                                              website TEXT,
                                              rating TEXT,
                                              address TEXT,
                                              phone TEXT,
                                              review_count TEXT,
                                              about TEXT,
                                              linkedin TEXT,
                                              youtube TEXT,
                                              twitter TEXT,
                                              facebook TEXT,
                                              xing TEXT,
                                              northdata TEXT,
                                              creditreform TEXT,
                                              side_bar_html TEXT,
                                              search_results TEXT
                                                );
                                        """).format(sqlpsycop.Identifier(data_table_history))

            # Execute the SQL command
            cursor.execute(create_table_query)

            # Commit the changes to the database
            db.commit()

        # Define the SQL command to select the next company to scrape
        select_next_company_query = sqlpsycop.SQL("""
                    SELECT company_id
                    FROM {}                    
                """).format(sqlpsycop.Identifier(data_table_updated))

        # Execute the SQL command to select the next company
        cursor.execute(select_next_company_query)

        # Fetch the result
        result = cursor.fetchall()

        if result:
            # Create a dictionary with column names as keys and result values as values
            result_dict = [i[0] for i in result]
        for comp_ids in df_dicts:
            if comp_ids['id'] not in result_dict:
                # Prepare SQL statement for inserting data
                sql = "INSERT INTO {} (name, company_id) VALUES (%s, %s)".format(data_table_updated)

                # Extract values from dictionaries and insert into database
                # Execute the insert operation in one go
                cursor.execute(sql, (comp_ids['name'], comp_ids['id']))
                # Commit the transaction
                db.commit()
                print('wait --- inserting')



        print('table already exit')

create_table_andinsert_data()

try:
    # Define the SQL command to select the next company to scrape
    select_next_company_query = sqlpsycop.SQL("""
                SELECT id
                FROM {}
                WHERE end_time is NULL           
            """).format(sqlpsycop.Identifier(data_table_updated))

    # Execute the SQL command to select the next company
    cursor.execute(select_next_company_query)

    # Fetch the result
    result_endtime = cursor.fetchall()

    if len(result_endtime) == 0:
        print('-----------------------------------------------------')
        print('!!!!!!!   All companies are scraped you want to scrape the data again!!! --------')
        print('-----------------------------------------------------')

        choice = input('Do You want to Run the script again pres y(YES) or n(NO) to continue !!!!')

        if choice == 'y':
            update_start_time_query = sqlpsycop.SQL("""
                        UPDATE {}
                        SET start_time = NULL,
                        end_time = NULL                ;
                    """).format(sqlpsycop.Identifier(data_table_updated))

            # Execute the SQL command to update start_time
            cursor.execute(update_start_time_query)

            # Commit the changes to the database
            db.commit()
        else:
            sys.exit()
except Exception as E:
    returntype = connection_db()
    db = returntype[0]
    cursor = returntype[1]
    pass


crawlera_api_key  = '6df5a833d5c74f71af44c7e7b56145d5'


def create_google_search_url(query):
    base_url = "https://www.google.com/search"
    query_params = {'q': query}
    encoded_query = urllib.parse.urlencode(query_params)
    search_url = f"{base_url}?{encoded_query}"
    return search_url


def get_next_company_to_scrape(table_name):
    try:
        # Define the SQL command to select the next company to scrape
        select_next_company_query = sqlpsycop.SQL("""
                    SELECT id,company_id, name
                    FROM {}
                    WHERE start_time IS NULL
                    ORDER BY id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED;
                """).format(sqlpsycop.Identifier(table_name))

        # Execute the SQL command to select the next company
        cursor.execute(select_next_company_query)

        # Fetch the result
        result = cursor.fetchone()

        if result:
            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Create a dictionary with column names as keys and result values as values
            result_dict = {column_names[i]: result[i] for i in range(len(column_names))}

            # Update the start_time to mark the company as in progress
            update_start_time_query = sqlpsycop.SQL("""
                        UPDATE {}
                        SET start_time = CURRENT_TIMESTAMP
                        WHERE id = {};
                    """).format(sqlpsycop.Identifier(table_name), sqlpsycop.Literal(result_dict['id']))

            # Execute the SQL command to update start_time
            cursor.execute(update_start_time_query)

            # Commit the changes to the database
            db.commit()

            # Return the company name
            return result_dict


    except Exception as e:
        print(f"Error: {e}")

def scraping_source(companyinfo):
    # google_search_url = create_google_search_url('Martin-Luther-Krankenhausbetrieb GmbH')
    google_search_url = create_google_search_url(companyinfo['name'])
    # google_search_url = create_google_search_url('incari Gmbh')
    driver.get(google_search_url)
    time.sleep(2)
    try:
        driver.find_element(By.CSS_SELECTOR, "button#L2AGLb").click()
        time.sleep(2)
    except:
        pass
    response_scrapy = scrapy.Selector(text=driver.page_source)
    item = dict()
    item['company_id'] = companyinfo['company_id']
    item['url'] = google_search_url
    try:
        item['side_bar_html'] = driver.find_element(By.CSS_SELECTOR, 'div[role="complementary"]').get_attribute('outerHTML')
    except:
        item['side_bar_html'] = ""
    try:
        item['industry'] = driver.find_element(By.CSS_SELECTOR, "span.YhemCb").text
    except:
        item['industry'] = ""
    if item['industry'] == "" or item['industry'] == None:
        try:
            item['industry'] = driver.find_element(By.CSS_SELECTOR, "span.E5BaQ").text
        except:
            item['industry'] = ""
        a=1
    try:
        item['website'] = [v.find_element(By.CSS_SELECTOR, "a").get_attribute('href') for v in driver.find_elements(By.CSS_SELECTOR, "div.QqG1Sd") if 'website' in v.text.lower()][0]
    except:
        item['website'] = ""
    if item['website'] == "" or item['website'] == None:
        try:
            item['website'] = [v.get_attribute('href') for v in driver.find_elements(By.CSS_SELECTOR,'div[data-attrid="kc:/local:unified_actions"] a') if v.text == 'Website'][0]
        except:
            item['website'] = ""
    if item['website'] == "" or item['website'] == None:
        try:
            item['website'] = driver.find_element(By.CSS_SELECTOR,'a[data-attrid="visit_official_site"]').get_attribute('href')
        except:
            item['website'] = ""
    try:
        item['name'] = companyinfo['name']
    except:
        item['name'] = ''
    try:
        item['rating'] = response_scrapy.css('span.Aq14fc ::text').get()
    except:
        item['rating'] = ''
    try:
        item['address'] = ''.join(response_scrapy.css('div[data-attrid="kc:/location/location:address"] ::text').extract()).replace('Adresse:',"").replace('Address:',"").strip()
    except:
        item['address'] = ''
    try:
        item['phone'] = ''.join(response_scrapy.css('div[data-attrid="kc:/local:alt phone"] ::text').extract()).replace('Telefon:',"").replace('Phone:',"").strip()
    except:
        item['phone'] = ''
    try:
        item['review_count'] = response_scrapy.css('div[data-attrid="kc:/collection/knowledge_panels/local_reviewable:star_score"] a ::text').get().replace("Rezensionen", "").strip()
    except:
        item['review_count'] = ""
    if item['review_count'] == "" or item['review_count'] == None:
        try:
            item['review_count'] = response_scrapy.css('div[data-attrid="subtitle"] a[data-async-trigger="reviewDialog"] ::text').get().replace("Rezensionen", "").replace('Google reviews',"").strip()
        except:
            item['review_count'] = ""



    try:
        driver.find_element(By.CSS_SELECTOR, 'a[aria-label="show more"]').click()
    except:
        pass

    try:
        item['about'] = driver.find_element(By.CSS_SELECTOR,'div[data-attrid="kc:/local:merchant_description"] div[jsname="EvNWZc"]').text
    except:
        item['about'] = ''

    for loop in response_scrapy.css('g-link.fl.w23JUc.ap3N9d a ::attr(href)').extract():
        if 'linkedin' in loop:
            item['linkedin'] = loop
        if 'youtube' in loop:
            item['youtube'] = loop
        if 'twitter' in loop:
            item['twitter'] = loop
        if 'facebook' in loop:
            item['facebook'] = loop
    if len(response_scrapy.css('g-link.fl.w23JUc.ap3N9d a ::attr(href)').extract()) == 0:
        for loop in response_scrapy.css('div[data-attrid="kc:/common/topic:social media presence"] a ::attr(href)').extract():
            if 'linkedin' in loop:
                item['linkedin'] = loop
            if 'youtube' in loop:
                item['youtube'] = loop
            if 'twitter' in loop:
                item['twitter'] = loop
            if 'facebook' in loop:
                item['facebook'] = loop


    # first_five_pages = []
    search_Results = []
    page = 1
    target_list = []
    title_list = []
    link_list = []

    while page < 6:
        response_scrapy = scrapy.Selector(text=driver.page_source)
        for index, link_loop in enumerate(response_scrapy.css('div.yuRUbf')):
            item1 = dict()
            try:
                if 'linkedin' in link_loop.css('a ::attr(href)').get():
                    if 'linkedin' not in list(item.keys()) or item['linkedin'] == '' or item['linkedin'] == None:
                        try:
                            item['linkedin'] = link_loop.css('a ::attr(href)').get()
                        except:
                            item['linkedin'] = ""
            except:
                pass

            try:
                if 'crunchbase' in link_loop.css('a ::attr(href)').get():
                    if 'crunchbase' not in list(item.keys()) or item['crunchbase'] == '' or item['crunchbase'] == None:
                        item['crunchbase'] = link_loop.css('a ::attr(href)').get()
            except:
                pass
            try:
                if 'facebook' in link_loop.css('a ::attr(href)').get():
                    if 'facebook' not in list(item.keys()) or item['facebook'] == '' or item['facebook'] == None:
                        item['facebook'] = link_loop.css('a ::attr(href)').get()
            except:
                pass

            try:
                if 'northdata' in link_loop.css('a::attr(href)').get():
                    if 'northdata' not in list(item.keys()) or item['northdata'] == '' or item['northdata'] == None:
                        item['northdata'] = link_loop.css('a ::attr(href)').get()
            except:
                pass
            try:
                if 'xing' in link_loop.css('a ::attr(href)').get():
                    if 'xing' not in list(item.keys()) or item['xing'] == '' or item['xing'] == None:
                        item['xing'] = link_loop.css('a ::attr(href)').get()
            except:
                pass
            try:
                if 'creditreform' in link_loop.css('a ::attr(href)').get():
                    if 'creditreform' not in list(item.keys()) or item['creditreform'] == '' or item['creditreform'] == None:
                        item['creditreform'] = link_loop.css('a ::attr(href)').get()
            except:
                pass
            try:
                if 'handelregister' in link_loop.css('a ::attr(href)').get():
                    if 'handelregister' not in list(item.keys()) or item['handelregister'] == '' or item['handelregister'] == None:
                        item['handelregister'] = link_loop.css('a ::attr(href)').get()
            except:
                pass

            if (link_loop.css('span.VuuXrf ::text').get() not in target_list) and (
                    link_loop.css('a ::text').get() not in title_list) and (
                    link_loop.css('a ::attr(href)').get() not in link_list):
                try:
                    item1['target'] = link_loop.css('span.VuuXrf ::text').get()
                    target_list.append(link_loop.css('span.VuuXrf ::text').get())
                except:
                    item1['target'] = ""

                try:
                    item1['title'] = link_loop.css('a ::text').get()
                    title_list.append(link_loop.css('a ::text').get())
                except:
                    item1['title'] = ""

                try:
                    item1['link'] = link_loop.css('a ::attr(href)').get()
                    link_list.append(link_loop.css('a ::attr(href)').get())
                except:
                    item1['link'] = ""
                try:
                    item1['text'] = ' '.join(link_loop.css('div.VwiC3b.yXK7lf ::text').extract())
                except:
                    item1['text'] = ""

                search_Results.append(item1)

        try:
            driver.find_element(By.CSS_SELECTOR, "a#pnnext").click()
            time.sleep(4)
        except:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            except:
                pass
            # break
        page = page + 1
        a = 1

    item['search_results'] = str(search_Results)
    check_list = ['industry', 'review_count', 'name', 'xing', 'creditreform', 'handelregister', 'rating', 'address',
                  'phone', 'about', 'linkedin', 'youtube', 'twitter', 'facebook', 'crunchbase', 'northdata',
                  'side_bar_html', 'search_results']

    for loop_check in check_list:
        if loop_check not in list(item.keys()):
            item[loop_check] = ""
    update_query = (
        f"UPDATE {data_table_updated} "
        "SET end_time= CURRENT_TIMESTAMP, name = %(name)s,company_id = %(company_id)s,industry=%(industry)s,website = %(website)s ,rating = %(rating)s, address = %(address)s, phone = %(phone)s, review_count = %(review_count)s, about = %(about)s, linkedin = %(linkedin)s, youtube = %(youtube)s, twitter = %(twitter)s, facebook = %(facebook)s, xing = %(xing)s,northdata = %(northdata)s, creditreform = %(creditreform)s, side_bar_html = %(side_bar_html)s, search_results = %(search_results)s "
        "WHERE company_id = %(company_id)s"
    )
    cursor.execute(update_query, item)
    db.commit()

    query = (
        f"INSERT INTO {data_table_history} "
        "(name, company_id, industry, url,website, rating, address, phone, review_count, about, linkedin, youtube, twitter, facebook, xing, northdata, creditreform, side_bar_html, search_results) "
        "VALUES (%(name)s, %(company_id)s, %(industry)s, %(url)s,%(website)s, %(rating)s, %(address)s, %(phone)s, %(review_count)s, %(about)s, %(linkedin)s, %(youtube)s, %(twitter)s, %(facebook)s, %(xing)s, %(northdata)s, %(creditreform)s, %(side_bar_html)s, %(search_results)s) "
    )
    # Execute the query with the data
    cursor.execute(query, item)
    db.commit()

    print('---Done---')

def check_blocked_rows(data_table_name):
    three_days_ago = datetime.now() - timedelta(minutes=1)
    # Construct the SQL query to select the next company
    select_next_company_query = sqlpsycop.SQL("""
            SELECT id,company_id,name
            FROM {}
            WHERE end_time IS NULL 
            AND start_time <= {}
            ORDER BY id
            LIMIT 1
        """).format(sqlpsycop.Identifier(data_table_name), sqlpsycop.Literal(three_days_ago))

    # Execute the query to select the next company
    cursor.execute(select_next_company_query)

    # Fetch the selected row
    selected_row = cursor.fetchone()
    if selected_row:
        # Get column names
        column_names_blocked_rows = [desc[0] for desc in cursor.description]

        # Create a dictionary with column names as keys and result values as values
        result_dict_blocked_rows = {column_names_blocked_rows[i]: selected_row[i] for i in range(len(column_names_blocked_rows))}

        update_end_time_query = sqlpsycop.SQL("""
                                      UPDATE {}
                                      SET 
                                      start_time = CURRENT_TIMESTAMP
                                      WHERE name=%s and id=%s;
                                  """).format(sqlpsycop.Identifier(data_table_name))

        # Execute the SQL command to update start_time
        cursor.execute(update_end_time_query, (result_dict_blocked_rows['name'], result_dict_blocked_rows['id']))

        # Commit the changes to the database
        db.commit()
        return result_dict_blocked_rows
    else:
        return []




driver = webdriver.Chrome()
driver.maximize_window()
while True:
    next_company = get_next_company_to_scrape(data_table_updated)
    if next_company != None:
        scraping_source(next_company)
    else:
        get_company_name_id = check_blocked_rows(data_table_updated)
        if len(get_company_name_id) > 0:
            scraping_source(get_company_name_id)
        else:
            print('-----------------------------------------------------')
            print('!!!!!!!  Great All companies are scraped !!! --------')
            print('-----------------------------------------------------')
            break

driver.quit()



