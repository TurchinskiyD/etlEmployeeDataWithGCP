import csv
import random
import string
from faker import Faker
from google.cloud import storage


# Ініціалізація Faker
fake = Faker('uk_UA')  # Для української локалі


def generate_phone_number():
    prefix = random.choice(["50", "66", "67", "73", "93", "95", "96", "97", "98", "99"])
    main_number = fake.random_number(digits=7, fix_len=True)
    return f"+380 {prefix} {str(main_number)[:3]}-{str(main_number)[3:5]}-{str(main_number)[5:]}"


# Список поштових доменів, що використовуються в Україні
ukrainian_domains = ["ukr.net", "gmail.com", "i.ua", "meta.ua", "mail.ua", "yahoo.com", "outlook.com"]


# Функція для генерації електронної пошти з українськими доменами
def generate_email():
    username = fake.user_name()
    domain = random.choice(ukrainian_domains)
    return f"{username}@{domain}"


# Функція для генерації пароля
def generate_password():
    len_pas = 10
    characters = string.ascii_letters + string.digits + random.choice("!@#$%^&*()")
    return ''.join(random.choices(characters, k = len_pas))


# Функція для генерації даних співробітників
def generate_employees(num_employees):
    employees = []
    positions = ["Менеджер", "Аналітик", "Розробник", "Дизайнер", "Тестувальник", "HR"]
    departments = ["Маркетинг", "Розробка", "Продажі", "IT", "Фінанси", "HR"]

    for _ in range(num_employees):
        employee = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": generate_email(),
            "Пароль": generate_password(),
            "phone_number": generate_phone_number(),
            "address": fake.city(),
            "position": random.choice(positions),
            "department": random.choice(departments),
            "start_date": fake.date_between(start_date='-5y', end_date='today').strftime('%d.%m.%Y'),
            "salary": random.randint(15000, 50000),
        }
        employees.append(employee)

    return employees


# Функція для збереження у CSV
def save_to_csv(dict_date, filename="employees_data.csv"):
    # Заголовки колонок
    fieldnames = employees[0].keys()

    # Запис у файл
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()  # Записати заголовки
        writer.writerows(dict_date)  # Записати дані

# Генерація даних
employees = generate_employees(500)
save_to_csv(employees)


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')


# Set your GCS bucket name and destination file name
bucket_name = 'employees-data'
source_file_name = 'employees_data.csv'
destination_blob_name = 'employees_data.csv'

# Upload the CSV file to GCS
upload_to_gcs(bucket_name, source_file_name, destination_blob_name)


# # Виведення результатів
# for employee in employees:
#     print(employee)
