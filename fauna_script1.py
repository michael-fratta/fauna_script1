# python scheduler
import schedule
import time

def job(): # define the whole script as a function

    from dotenv import load_dotenv
    import os
    load_dotenv()

    # sftp connection pre-amble
    import pandas as pd
    from datetime import datetime
    import pysftp
    import requests

    today = datetime.today().strftime('%d%m%Y 08-30')

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys=None

    hostname = os.getenv('hostname')
    username = os.getenv('username')
    password = os.getenv('password')

    # get all files in sftp
    print("Getting all files on SFTP...\n")
    files = pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts).listdir()

    # initialise file_found bool
    file_found = False

    # initialise slackbot func
    slack_token = os.getenv('slack_password')
    slack_channel = '#script-alerts'
    # create func
    def post_message_to_slack(text):
        return requests.post('https://slack.com/api/chat.postMessage', {
            'token': slack_token,
            'channel': slack_channel,
            'text': text,
        }).json()

    # if a file is at an expected time (of a given day) - use that file, else if it's later than that - use that file

    # convert today back to datetime obj
    today_obj = datetime.strptime(today, '%d%m%Y %H-%M')

    for file in files:
        if file == f'Ratebook_ ({today}).csv':
            with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
                print("Ignore the above warning!\n")
                print("Connection succesfully established...")
                with sftp.open(file) as f:
                    new_rb = pd.read_csv(f)
                    print(f"Successfully read {file}!\n")
                    file_found = True
        else:
            try:
                if "Ratebook_" in file:
                    datetime_str = file.replace('Ratebook_ (',"").replace(').csv',"")
                    datetime_obj = datetime.strptime(datetime_str, '%d%m%Y %H-%M')
                    if datetime_obj > today_obj:
                        with pysftp.Connection(host=hostname, username=username, password=password, cnopts=cnopts) as sftp:
                            print("Ignore the above warning!\n")
                            print("Connection succesfully established...")
                            print("Getting the latest Ratebook...")
                            with sftp.open(file) as f:
                                new_rb = pd.read_csv(f)
                                print(f"Successfully read {file}!\n")
                                file_found = True
            except:
                pass

    if file_found:
        ## sort df by cap code, then by term, then by mileage
        new_rb.sort_values(by=['CAPCode','Term','Mileage'],inplace=True)

        ## reset index of sorted df (otherwise when I print everything it gies by i and will not print in order)
        new_rb.reset_index(drop=True,inplace=True)

        ### transform ratebook from csv into cars_list

        # initialise cars list
        cars_list = []

        # initialise list idx
        idx = -1

        # first, cycle through all rows in car ratebook
        for i in range(len(new_rb)):
            
            # make it faster to search
            loc = new_rb.loc[i]
            
            # then, assign relevant vars with their respective vals
            cap_code = loc['CAPCode'].replace(' ', '')
            manufacturer = loc['Manufacturer'].lower()
            model = loc['Model Name'].lower()
            variant = loc['Variant']
            cap_id = int(loc['CAP ID'])
            if str(loc['Insurance Group']) != "nan":
                insurance_group = loc['Insurance Group']
            else:
                insurance_group = "0" 
            model_year = float(loc['Model Year'])
            
            # from prices array:
            term_months = int(loc['Term'])
            quote_mileage = int((int(loc['Mileage'])/term_months)*12)
            price_pence = round(loc['Finance Rental']*100)
            finance_rental_pence = round(loc['Finance Rental']*100)
            service_rental_pence = round(loc['Non Finance Rental']*100)
            p11d_pence = round(loc['P11D']*100)
            excess_ppm = float(loc['Excess PPM Over'])
            
            lender_name = "car_lender" 
            
            # initialise cars dict
            cars = {}
            
            # assign car_specs dicts
            car_specs = {"cap_code": cap_code,
                        "manufacturer": manufacturer,
                        "model": model,
                        "variant": variant,
                        "cap_id": cap_id,
                        "insurance_group": insurance_group,
                        "model_year": model_year,
                        "prices":{lender_name:[]}}
            
            # assign car_prices dicts
            car_prices = [{"term_months": term_months,
                        "quote_mileage": quote_mileage,
                        "price_pence": price_pence,
                        "finance_rental_pence": finance_rental_pence,
                        "service_rental_pence": service_rental_pence,
                        "p11d_pence": p11d_pence,
                        "excess_ppm": excess_ppm},]
            
            # initialise exists to false
            exists = 0
            # see if capcode already in cars_list
            if len(cars_list) > 0:
                if cap_code in cars_list[idx]['cap_code']:
                    exists = 1
                else: exists = 0
            
            if exists == 0: # if cap_code not in cars_list
                if cars_list != None: # if cars_list is not empty
                    cars_list.append(cars) # append cars obj to cars_list li
                    cars.update(car_specs) # update cars obj with car_specs dict
                    cars['prices'][lender_name]+=car_prices #update cars obj with car_prices dict
                    idx+=1
                else: # if cars_list is empty
                # update the car obj
                    cars.update(car_specs)
                    cars['prices'][lender_name]+=car_prices
                    idx+=1
                
            else: # if cap_code already in cars_list
                cars_list[idx]['prices'][lender_name]+=car_prices # just append relevant car_prices to cars obj

        # connect to FaunaDB
        from faunadb import query as q
        from faunadb.client import FaunaClient
        client = FaunaClient(secret=os.getenv('secret'),domain="db.fauna.com",port=443,scheme="https")

        # create obj referencing 'car_objects' collection in 'car_ratebooks' db in FaunaDB
        query = client.query( # query starts with 'data' and contains array of objects that start with 'ref', then 'data' again
            q.map_(q.lambda_(["X"], q.get(q.var("X"))),
                q.paginate(q.documents(q.collection('car_objects')),size=100000)))

        # create a list of fauna caps and ref ids for easier iteration
        caps_refs = []
        for car in query['data']:
            caps_refs.append([car['data']['cap_code'],car['ref'].id()])

        # create a list containing fauna caps for easier iteration
        fauna_caps = []
        for car in query['data']:
            fauna_caps.append(car['data']['cap_code'])

        ### create list of cars not in fauna, and matching cars from new rb
        new_cars_rb = []
        same_cars_rb = []
        for car in cars_list:
            if car['cap_code'] not in fauna_caps:
                new_cars_rb.append(car)
            else:
                same_cars_rb.append(car)

        # initiate refresh variable to hold False in case no refresh is required after checks
        refresh = False

        # initialise list of created cars
        created_cars = []

        # for each car in new cars - create a  new car obj #
        # just collect the data as above and make a new car obj for it ##
        for car in new_cars_rb:
            client.query(q.create(q.collection('car_objects'),{"data": car}))
            print(car['cap_code'],"was created!")
            created_cars.append(car['cap_code'])
            refresh = True

        ### i have a same_cars list of object, which is a list made of new rb rates, if they match with fauna
        ### these are sorted alphabetically
        ### however, same_cars will not necessarily be the same length as the fauna query
        ### to use iteration reliably - i need to create a fauna query which contains only the same_cars
        # create list of caps in same_cars
        same_cars_caps = []
        for car in same_cars_rb:
            same_cars_caps.append(car['cap_code'])

        ## create list of fauna objs only if there is a respective cap in new rb
        same_cars_fauna = []
        for car in query['data']:
            if car['data']['cap_code'] in same_cars_caps:
                same_cars_fauna.append(car)
                
        ### create a list of fauna objs without the ref part; to easily compare with new rb rates
        same_cars_fauna_no_refs = []
        for car in same_cars_fauna:
            same_cars_fauna_no_refs.append(car['data'])

        ### sort same_cars_fauna_no_refs by cap_code
        from operator import itemgetter
        same_cars_fauna_no_refs_sorted = sorted(same_cars_fauna_no_refs, key=itemgetter('cap_code'))
        same_cars_rb_sorted = sorted(same_cars_rb, key=itemgetter('cap_code'))

        ### get list of unmatching prices cap_codes
        unmatched_prices_caps = []

        idx = 0
        for car in same_cars_fauna_no_refs_sorted:
            try: # if there is a lender_name already
                if car['prices'][lender_name] != same_cars_rb_sorted[idx]['prices'][lender_name]:
                    unmatched_prices_caps.append(car['cap_code'])
            except: # if there is no lender_name already
                unmatched_prices_caps.append(car['cap_code'])
            idx+=1

        ### get list of unmatching insurance groups
        unmatched_insurance_caps = []
        idx = 0
        for car in same_cars_fauna_no_refs_sorted:
            if car['insurance_group'] != same_cars_rb_sorted[idx]['insurance_group'] and car['insurance_group'] != 'nan':
                unmatched_insurance_caps.append(car['cap_code'])
            else:
                pass
            idx+=1

        # initialise list of updated cars
        updated_cars = []
        # for car in same_cars_rb; if car cap code is in unmatched_prices_caps - then get that car's prices from same_cars_rb,
        # and update fauna accordingly
        for car in same_cars_rb:
            if car['cap_code'] in unmatched_prices_caps:
                for pair in caps_refs:
                    if pair[0] == car['cap_code']:
                        ref = pair[1] # assign relevant ref
                new_rates = car['prices'][lender_name]
                # update the respective prices: lender_name
                client.query(q.update(q.ref(q.collection('car_objects'),ref),
                {'data':{'prices':{lender_name:new_rates}}}))
                updated_cars.append(car['cap_code'])
                refresh = True

        # initialise list of insurance updates
        insurance_updates = []
        # update the insurance if need be
        for car in same_cars_rb:
            if car['cap_code'] in unmatched_insurance_caps:
                for pair in caps_refs:
                    if pair[0] == car['cap_code']:
                        ref = pair[1]
                new_ins = car['insurance_group']
                client.query(q.update(q.ref(q.collection('car_objects'),ref),
                {'data':{'insurance_group':new_ins}}))
                insurance_updates.append(car['cap_code'])
                print(car['cap_code'],"had its insurance updated to",new_ins)
                
        ### finally - clear the cache, if there is a need to

        if refresh:

            if len(created_cars) > 0:
                if len(created_cars) == 1:
                    print(f"The following car was created in Fauna: {created_cars}")
                else:
                    created_cars.sort()
                    print(f"The following {len(created_cars)} cars were created in Fauna: {created_cars}")

            if len(updated_cars) > 0:
                if len(updated_cars) == 1:
                    print(f"The following car was updated in Fauna: {updated_cars}")
                else:
                    updated_cars.sort()
                    print(f"The following {len(updated_cars)} cars were updated in Fauna: {updated_cars}")

            if len(insurance_updates) > 0:
                if len(insurance_updates) == 1:
                    print(f"The following car had its insurance_group updated in Fauna: {insurance_updates}")
                else:
                    print(f"The following {len(insurance_updates)} cars had their insurance_group updated in Fauna: {insurance_updates}")

            import urllib.request
            webURL = urllib.request.urlopen(os.getenv('cache_url'))
            if webURL.getcode() == 200:
                print("refresh was successful!")
            else:
                slack_info = "Something went wrong while trying to refresh cache following script update! Check logs on Heroku for further details!"
                post_message_to_slack(slack_info)
        else:
            print("No cars needed updating!")

    else:
        print("No file was found!\n")
        # what time and day is it now?
        now = time.localtime()
        if now.tm_wday == 0 and now.tm_hour > 13: # if it's monday after 1pm
            slack_info = f"A file was not found, but it should have been! Something might be wrong with Key2. Check script logs on Heroku for further details!"
            post_message_to_slack(slack_info)
            print(slack_info)

    ### END OF JOB ###

# run script every hour at 35 mins past the hour
schedule.every().hour.at(":35").do(job)
while True:
    schedule.run_pending()
    time.sleep(1)
