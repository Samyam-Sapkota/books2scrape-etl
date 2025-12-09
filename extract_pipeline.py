def fetch_info_from_individual_url(pagelink):
    import pandas as pd
    from bs4 import BeautifulSoup
    import requests
    response = requests.get(pagelink).text
    # print(f" link {response}")
    soup = BeautifulSoup(response, 'html.parser')

    title = soup.find('h1').text
    description_tag = soup.find('p', class_=False)
    description_tag = description_tag.text if description_tag else "No description available"
    category_of_book = soup.find('ul', class_='breadcrumb').find_all('li')[2].find('a').text
    image_link = f"http://books.toscrape.com/{soup.find('div', class_='item active').find('img')['src'].replace('../','')}"
    
    rating_tag = soup.find('p', class_='star-rating')
    rating= find_ratings(rating_tag['class'][1])

    table_data = soup.find('table').find_all('td')
    table_data = [i.text for i in table_data]
    
    UPC = table_data[0]
    product_type = table_data[1]
    price_excl_tax = table_data[2]
    price_incl_tax = table_data[3]
    tax = table_data[4]
    no_of_reviews = table_data[6]

    stock_data = table_data[5]
    stock_availability = lambda stock: True if "In stock" in  stock_data.split('(')[0].strip() else False 
    no_of_books_in_stock = lambda stock: int(stock_data.split('(')[1].replace('available)','').strip())

    stock_availability= stock_availability(stock_data)
    no_of_books_in_stock= no_of_books_in_stock(stock_data)
    

    data = {
        'Title': title,
        'Description': description_tag,
        'Category': category_of_book,
        'Image_link': image_link,
        'Is_in_Stock': stock_availability,
        'No_of_books_in_Stock': no_of_books_in_stock,
        'Rating': rating,
        'UPC': UPC,
        'Product Type': product_type,
        'Price (excl. tax)': price_excl_tax,
        'Price (incl. tax)': price_incl_tax,
        'Tax': tax,
        'Number of reviews': no_of_reviews
    }
    return data

    

    

def fetch_books_urls_from_main_page(main_page_url):
    # pass
    import pandas as pd
    from bs4 import BeautifulSoup
    import requests

    data = requests.get(main_page_url).text
    pages_soup = BeautifulSoup(data, 'html.parser')
    all_article_tags = pages_soup.find_all('article')
    link_for_each_book = []

    for i in range(0,20):
        full_link = "http://books.toscrape.com/catalogue/"+all_article_tags[i].find('h3').find('a')['href']
        link_for_each_book.append(full_link)

    
    return link_for_each_book


def fetch_main_page_url(no_of_pages):
    import pandas as pd
    print("Starting the scraping process...")

    data = []
    for i in range(1, no_of_pages + 1):
        print(f"Processing page {i} of {no_of_pages}...")
        for single_book in fetch_books_urls_from_main_page(f"http://books.toscrape.com/catalogue/page-{i}.html"):
          
            if no_of_pages == 1:
                print(f"Fetching data for book: {single_book}")  
            data.append(fetch_info_from_individual_url(single_book))
    df = pd.DataFrame(data)
    df.to_csv("books.csv", index=False)
    print("Scraping completed and data saved to books.csv")

def find_ratings(data):
    mapping = {"One":1, "Two":2, "Three":3, "Four":4, "Five":5}
    return mapping.get(data, 0)


# print("Scraping completed and data saved to books.csv")