import requests
import pandas as pd
from pyexpat import model
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


DATABASE_URL = "postgresql://postgres:erp1757070265@localhost/digikala.db"

def fetch_mobile_data():
    url = "https://api.digikala.com/v1/categories/mobile-phone/brands/samsung/search/?page=1"  # URL فرضی، آن را با API واقعی جایگزین کنید
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    size = data['data']['pager']['total_pages']
    print(size)
    all_products = []
    all_ids = []
    for page in range(size):
        url = f"https://api.digikala.com/v1/categories/mobile-phone/brands/samsung/search/?page={page+1}"
        response = requests.get(url)
        data = response.json()
        products = data['data']['products']
        for product in products:
            all_products.append({
                'id': product['id'],
                'model': product['title_fa']
            })
        #print(model)
        #print(id)
    print(all_products)



def clean_data(all_products):
   df = pd.DataFrame(all_products)
   return df

   df = df[['id', 'model']]


def save_to_database(df):
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # ایجاد جدول در صورت عدم وجودv
    #df.to_sql('mobiles', con=engine, if_exists='replace', index=False)  # یا 'append' برای اضافه‌کردن

    for row in df:
        # بررسی وجود سطر
        existing = session.execute(f"SELECT * FROM products WHERE id = {row['id']}").fetchone()
        if existing:
            # ویرایش سطر
            queryUpdate = 'UPDATE "final"."digikala"."products" SET "model" = :model WHERE "id" = :id'
            session.execute(queryUpdate)
        else:
            # اضافه کردن سطر جدید
            queryInsert = 'INSERT INTO "final"."digikala"."products" ("model", "id") VALUES (%S, %S)'
            values = {'model': model, 'id': row['id']}
            print(queryInsert)
            session.execute(queryInsert, values)

    session.commit()
    session.close()
print("eli")

# تابع اصلی
def main():
    raw_data = fetch_mobile_data()
    cleaned_data = clean_data(raw_data)
    save_to_database(cleaned_data)


if __name__ == "__main__":
    main()