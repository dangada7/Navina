import pandas as pd
import datetime
import boto3


# contains_the_number
def contains_the_number(str):
    if str.find("1") > -1:
        return 1
    elif (str.find("2") > -1 or str.find("twice") > -1):
        return 2
    elif str.find("3") > -1:
        return 2
    elif str.find("4") > -1:
        return 2
    elif str.find("5") > -1:
        return 2
    elif str.find("6") > -1:
        return 2
    elif str.find("7") > -1:
        return 2
    else:
        return 1

# type_of_frequency
# return the number of days the type of frequency represents
# month = num/30, hour = 24/num...
def type_of_frequency(str):

    if str.find("bid") > -1:
        return 2
    elif str.find("hour") > -1:
        return 24 / contains_the_number(str)
    else:
        return contains_the_number(str)

# 1) read scv
df = pd.read_csv('medications_interview_input.CSV')
df['prescription_date'] = pd.to_datetime(df['prescription_date'], format="%d/%m/%Y")

# 2) get last year
now = datetime.datetime.now()
days = datetime.timedelta(365)
new_date = now - days

# 3) drop prescription_date
df = df[(df['prescription_date'] > new_date)]

# 4) drop duplicates ATC
df = df.drop_duplicates(subset=['ATC', 'NID'])

# 5) aws comprehend medical
client = boto3.client(service_name='comprehendmedical', region_name='us-east-1')
for index, row in df.iterrows():
    result = client.detect_entities(Text=row['prescription_description'])
    entities = result['Entities']
    UnmappedAttributes = result['UnmappedAttributes']

    # 5.1) quantity
    quantity = row['quantity']

    # 5.2) days_from_prescription_date
    delta = datetime.datetime.now() - row['prescription_date']
    days_from_prescription_date = delta.days

    # 5.3) frequency and dosage
    frequency_number = 1
    dosage_per_day = 1
    frequency_text = ''
    for UnmappedAttribute in UnmappedAttributes:
        type = UnmappedAttribute['Attribute']['Type']

        # 5.3.1) frequency_number
        if type == 'FREQUENCY':
            frequency_text = UnmappedAttribute['Attribute']['Text'].lower()
            frequency_number = type_of_frequency(frequency_text)

        # 5.4.2) dosage_per_Day
        if type == 'DOSAGE':
            dosage_text = UnmappedAttribute['Attribute']['Text'].lower()
            dosage_per_day = contains_the_number(dosage_text)

    # 5.5) day_left
    max_days = quantity / (frequency_number * dosage_per_day)
    day_left = max_days - days_from_prescription_date

    # 5.6) update should_refill, days_left and frequency columns
    if days_from_prescription_date < 0:
        print('didnt start taking pills')
        df.loc[index, 'frequency'] = frequency_text
        df.loc[index, 'should_refill'] = 'FALSE'
        df.loc[index, 'days_left'] = 'didnt start taking pills'
    else:
        print('ok', day_left <= 0, day_left)
        df.loc[index, 'frequency'] = frequency_text
        df.loc[index, 'should_refill'] = day_left <= 0
        df.loc[index, 'days_left'] = day_left

# 6) remove unnecessary columns
# del df['prescription_date']
del df['prescription_description']

# 7) write final csv
df.to_csv('output.csv', sep=',')

