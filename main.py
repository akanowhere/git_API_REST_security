from requests_oauth2client import *
import json
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor
from time import sleep




lock = threading.Lock()
# METER_WORKERS = 30
METER_WORKERS = 50

token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkVCNjk3RjU0MUExQzRBMzVBMjMwNzQ1QkVBRTAyMEExNDI3Njg1M0EiLCJ0eXAiOiJKV1QiLCJ4NXQiOiI2MmxfVkJvY1NqV2lNSFJiNnVBZ29VSjJoVG8ifQ.eyJuYmYiOjE2ODkxODQyOTMsImV4cCI6MTc1NDg4NDI5MywiaXNzIjoiaHR0cHM6Ly9sb2dpbi5wYW5kYXBlLmNvbS5iciIsImF1ZCI6WyJodHRwczovL2xvZ2luLnBhbmRhcGUuY29tLmJyL3Jlc291cmNlcyIsIkV4dGVybmFsUmVxdWVzdEFwaSIsIlBhbmRhcGVBcGkiXSwiY2xpZW50X2lkIjoiU2VjdXJpdHlTZWN1cmFuY2FBcGlDbGllbnQiLCJJZENvbXBhbnkiOiIxNTg4Iiwic2NvcGUiOlsiRXh0ZXJuYWxSZXF1ZXN0QXBpIiwiUGFuZGFwZUFwaSJdfQ.PwwfTdxt3oM8YxO4jZ7Be2jM5OXmuIXtHYojwCWiUy7rjeQP50AgDB-3NGJdB9TDTtrNu1tntClw76QG48LQFrGvxMKsHQr4KJQ0zViHL_LZv18-adHjQPl2jUIqJSmNMSWMGGIlBNVJve_K4mCTAnw7V57gO7Z4KtimBAe-rwaI9hoGQ2utTywSldHPO-JRIjdKte0b-qz29mpyyAj-OCPJsCTAi2zbEAVbQpfLSrXEu93SwU4B8SAV0dOUWptkPwXLjc7k8cMq2SRyYPihyqYdN2W9gkTUOKackgz_hWUQNoFSB7aT5A12VExkCoU6F9QK7XmzRZyz6d-UjCwydCuCPWQbQ1pAbE7pk3nukhaJxrydaxJR8tLua4K3iwj2vpkXP0pPiEf5v-f_385Ck70tY7IPI0quiwia6iNw1Oalm8TyGX2bWZyQusQifSXU4qlHwJDOeYJ7J2JTlD01i2-zcL2Edbh9VBg8qonD0Z15XVddR8dGZzygXPyRX1xm_4zFsrP2hFPVF_MBacklgIueKkjQ6O5TOZsr54P1FBlX2k1i08yBnbyn5A151wA69EWrNTdvXx8JeRqdpN_XniGR-NTKHYflJsdU89gk2Mv_eRORQvs0BWzthmEMSfSZ4anxysaCz_NcWUZgOEx2Ps1uezDS2ul5cKGKC5iq8VM"
page = [1, 2, 3, 4, 5, 6]



def get_requests():



    print("Iniciado get_requests")
    data = []
    resp = requests.get('https://api.pandape.com.br/v2/requests', auth=BearerAuth(token)).json()
    data.append(resp)
    with open("D:/pythonDSA/Security/python/source/log" + "/requests" + ".json", "w") as jsonFile:
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    df = pd.DataFrame.from_records(data[0]).drop_duplicates()
    df['idVacancyAssociated'] = df['idVacancyAssociated'].astype(str)
    df['idVacancyAssociated'] = df['idVacancyAssociated'].str.replace('\.0', ' ')
    df = df.replace(r'nan', ' ', regex=True)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/requests.csv', encoding='utf-8-sig', index=False,
              sep=';')
    data.clear()
    for c in range(len(df['idRequest'])):
        data.append(df['idRequest'][c])
    df1 = pd.DataFrame(data,columns=['Requisição'])
    df1.to_excel(r'D:/pythonDSA/Security/onedrive/SECURITY VIGILANCIA PATRIMONIAL LTDA/Controle de Vagas - General/Pós-Panda/requests_id.xlsx', encoding='utf-8-sig', index=False)
    print("Finalizado get_requests")
    return data

# D:\pythonDSA\Security\onedrive\SECURITY VIGILANCIA PATRIMONIAL LTDA\Controle de Vagas - General\Pós-Panda\Controle_Vagas.xlsx

def get_vancacies():

    print("Iniciado get_vancacies")
    data = []
    dataaux = []
    for n in page:
        page2 = (str(n))
        resp = requests.get('https://api.pandape.com.br/v2/vacancies?Page='+page2+'&PageSize=1000', auth=BearerAuth(token)) .json()
        data.append(resp['items'])
    with open("D:/pythonDSA/Security/python/source/log" + "/vacancies" + ".json", "w") as jsonFile:
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    for c in range(len(data)):
        dataaux.append(data[c])
    finallist = dataaux[0] + dataaux[1] + dataaux[2]
    df = pd.DataFrame.from_records(finallist)
    df = df.replace(r'\r\n\r\n', ' ', regex=True)
    df = df.replace(r'\t', ' ', regex=True)
    df = df.replace(r'\r\n', ' ', regex=True)
    df = df.replace(r'•', ' ', regex=True)
    df = df.replace(r';\r\n', ' ', regex=True)
    df = df.replace(r';', ' ', regex=True)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/vacancies.csv', encoding='utf-8-sig', index=False,
              sep=';')
    print("Finalizado get_vancacies")
    return data


def get_id_requests(data):

    print("Iniciado get_id_requests")
    dataaux = []
    for c in range(len(data)):
        page4 = (str(data[c]))
        file = 'https://api.pandape.com.br/v2/requests/' + page4
        try:
            resp = requests.get('https://api.pandape.com.br/v2/requests/' + page4, auth=BearerAuth(token)).json()
            dataaux.append(resp)
        except:
            print('TimeoutError: [WinError 10060] Uma tentativa de conexão falhou')
            sleep(2)
            resp = requests.get('https://api.pandape.com.br/v2/requests/' + page4, auth=BearerAuth(token)).json()
            dataaux.append(resp)
    with open("D:/pythonDSA/Security/python/source/log" + "/get_id_requests" + ".json", "w") as jsonFile:
        json.dump(dataaux, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    df = pd.DataFrame.from_records(dataaux)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/get_id_requests.csv', encoding='utf-8-sig', index=False,
              sep=';')
    print("Finalizado get_id_requests")


def get_id_vacancies():

    print("Iniciado get_id_vacancies")
    data = []
    dataaux = []
    dataaux1 = []
    resp = requests.get('https://api.pandape.com.br/v2/requests', auth=BearerAuth(token)).json()
    data.append(resp)
    with open("D:/pythonDSA/Security/python/source/log" + "/requests" + ".json", "w") as jsonFile:
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    df = pd.DataFrame.from_records(data[0]).drop_duplicates()
    df['idVacancyAssociated'] = df['idVacancyAssociated'].astype(str)
    df['idVacancyAssociated'] = df['idVacancyAssociated'].str.replace('\.0', ' ')
    df = df.replace(r'nan', ' ', regex=True)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/requests.csv', encoding='utf-8-sig', index=False,
              sep=';')
    data.clear()
    for c in range(len(df['idVacancyAssociated'])):
        if (df['idVacancyAssociated'][c]) != ' ':
            data.append(df['idVacancyAssociated'][c])
    for c in range(len(data)):
        page3 = (str(data[c]))
        file = 'https://api.pandape.com.br/v2/requests?idVacancy=' + page3
        try:
            resp = requests.get('https://api.pandape.com.br/v2/requests?idVacancy=' + page3,
                                auth=BearerAuth(token)).json()
            dataaux.append(resp)
        except:
            print('TimeoutError: [WinError 10060] Uma tentativa de conexão falhou')
            sleep(2)
            resp = requests.get('https://api.pandape.com.br/v2/requests?idVacancy=' + page3,
                            auth=BearerAuth(token)).json()
            dataaux.append(resp)
    with open("D:/pythonDSA/Security/python/source/log" + "/get_id_vacancies" + ".json", "w") as jsonFile:
        json.dump(dataaux, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    with open("D:/pythonDSA/Security/python/source/log/get_id_vacancies.json", "r+") as jsonFile:
        datajson = json.load(jsonFile)
    for c in range((len(datajson))):
        dataaux.append(datajson[c])
    for c in range(len(dataaux)):
        dataaux1.append(dataaux[c][0])
    df = pd.DataFrame.from_records(dataaux1).drop_duplicates()
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/get_id_vacancies.csv', encoding='utf-8-sig', index=False,
              sep=';')
    print("Finalizado get_id_vacancies")


def get_id_vacancies_all(data):

    print("Iniciado get_id_vacancies_all")
    dataaux = []
    for c in range(len(data)):
        page5 = (str(data[c]))
        file = 'https://api.pandape.com.br/v2/vacancies/' + page5
        try:
            resp = requests.get('https://api.pandape.com.br/v2/vacancies/' + page5, auth=BearerAuth(token)).json()
            dataaux.append(resp)
        except:
            print('TimeoutError: [WinError 10060] Uma tentativa de conexão falhou')
            sleep(2)
            resp = requests.get('https://api.pandape.com.br/v2/vacancies/' + page5, auth=BearerAuth(token)).json()
            dataaux.append(resp)
    with open("D:/pythonDSA/Security/python/source/log" + "/get_id_vacancies_all" + ".json", "w") as jsonFile:
        json.dump(dataaux, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    df = pd.DataFrame.from_records(dataaux)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/get_id_vacancies_all.csv', encoding='utf-8-sig', index=False,
              sep=';')
    df = pd.read_csv('D:/pythonDSA/Security/python/source/log/get_id_vacancies_all.csv', sep=';')
    df1 = pd.read_csv('D:/pythonDSA/Security/python/source/log/id_users.csv', sep=';')
    df['idUser'] = df['idUser'].map(df1.set_index('idCompanyUser')['name'] + " " + df1.set_index('idCompanyUser')['surname'])
    df.drop(['reference', 'job', 'idCategory1', 'idCategory2', 'idManagerialLevel', 'description', 'numberVacancies',
             'idContractWorkType', 'idWorkingHours', 'idWorkMethod', 'salaryMin', 'salaryMax', 'hideSalary', 'cep',
             'vacancyLocationType', 'companyHidden', 'alternativeDescription', 'youtubeVideoUrl', 'idStudy1Min',
             'idExperienceRange', 'ageMin', 'ageMax', 'idSex', 'deficiencyRequired', 'deficiencyInformation',
             'cidRequired', 'deficiencies', 'changeResidenceAvailabilityRequired', 'travelAvailabilityRequired',
             'vehicleRequired', 'licenseRequired', 'idLicenseList', 'studies', 'languages', 'benefits', 'skills',
             'tags', 'publish'], axis='columns', inplace=True)
    df2 = pd.read_csv('D:/pythonDSA/Security/python/source/log/get_id_requests.csv', sep=';')
    df['idRequest'] = df['idRequestToAssociate'].map(df2.set_index('idRequest')['status'])
    df.to_excel(
        r'D:/pythonDSA/Security/onedrive/SECURITY VIGILANCIA PATRIMONIAL LTDA/Controle de Vagas - General/Pós-Panda/requests_id.xlsx',
        encoding='utf-8-sig', index=False)
    print("Finalizado get_id_vacancies_all")


def custom_fields():

    print("Iniciado custom_fields")
    listdict20 = []
    datateste3 = []
    listadic = []
    listadic2 = []
    listaid100 = []
    listaid101 = []
    listaid4 = []
    with open("D:/pythonDSA/Security/python/source/log/get_id_requests.json", "r+") as jsonFile:
        datajson30 = json.load(jsonFile)
        #print(datajson30)
        for ii in range(len(datajson30)):
            dicustom = {}
            dicustom.update(datajson30[ii])
            del dicustom['name']
            del dicustom['reference']
            del dicustom['insertDate']
            del dicustom['numberVacancies']
            del dicustom['startDate']
            del dicustom['area']
            del dicustom['specialization']
            del dicustom['managerialLevel']
            del dicustom['departmentName']
            del dicustom['reason']
            del dicustom['contractWorkType']
            del dicustom['workingHours']
            del dicustom['salary']
            del dicustom['address']
            del dicustom['description']
            del dicustom['benefits']
            del dicustom['additionalInformation']
            del dicustom['profile']
            del dicustom['externalCode']
            del dicustom['deficiencyRequired']
            del dicustom['leads']
            del dicustom['documents']
            del dicustom['status']
            listdict20.append(dicustom)
    for c in range(len(listdict20)):
        listadic.append(listdict20[c])
        listadic2.append(listadic[c]['customFields'])
        listaid100.append(listadic[c]['idRequest'])
        listaid101.append(listaid100[c])
    for d in range(len(listadic2)):
        listaid4.append(listadic2[d])
    for g in range(len(listaid101)):
        datateste = [listaid100[g]] = {'idRequest': listaid100[g]}
        for gg in range(len(listaid4[0])):
            datateste4 = datateste.copy()
            try:
                datateste4.update(listaid4[g][gg])
                datateste3.append(datateste4)
            except:
                pass
    df = pd.DataFrame.from_records(datateste3).drop_duplicates()
    df = df.replace(r'\r\n\r\n', ' ', regex=True)
    df = df.replace(r'\t', ' ', regex=True)
    df = df.replace(r'\r\n', ' ', regex=True)
    df = df.replace(r'•', ' ', regex=True)
    df = df.replace(r';\r\n', ' ', regex=True)
    df = df.replace(r';', ' ', regex=True)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/custom_fields.csv',encoding='utf-8-sig', index=False,
                sep=';')
    df1 = df.pivot_table(index='idRequest', columns='label', values='value',aggfunc='first').reset_index()
    df1.to_csv(r'D:/pythonDSA/Security/python/source/log/custom_fields_final.csv', encoding='utf-8-sig', index=False,
              sep=';')
    print("Finalizado custom_fields")


def id_users():

    print("Iniciado id_users")
    data = []
    lista100 = []
    file = 'https://api.pandape.com.br/v2/company/users?Page=1&PageSize=1000'
    resp4 = requests.get('https://api.pandape.com.br/v2/company/users?Page=1&PageSize=1000',
                         auth=BearerAuth(token)).json()
    data.append(resp4)
    with open("D:\pythonDSA\Security\python\source\log" + "\id_user" + ".json", "w") as jsonFile:
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    with open("D:\pythonDSA\Security\python\source\log\id_user.json", "r+") as jsonFile:
        datajson4 = json.load(jsonFile)
        lista100.append([d['users'] for d in datajson4 if 'users' in d])
    df = pd.DataFrame.from_records(lista100[0][0]).drop_duplicates()
    df.to_csv(r'D:/pythonDSA/Security/python/Source/log/id_users.csv', encoding='utf-8-sig', index=False, sep=';')
    print("Finalizado id_users")


def data_source():

    print("Iniciado data_source")
    data = []
    dataaux = []
    dataaux1 = []
    dataaux2 = []
    dataaux3 = []
    resp8 = requests.get('https://api.pandape.com.br/v2/custom-fields', auth=BearerAuth(token)).json()
    with open("D:/pythonDSA/Security/python/source/log" + "/dim_custom_fields" + ".json", "w") as jsonFile:
        json.dump(resp8, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    data.append(resp8)
    df = pd.DataFrame.from_records(data[0])
    df['idDatasource'] = df['idDatasource'].astype(str)
    df['idDatasource'] = df['idDatasource'].str.replace('\.0', ' ')
    df = df.replace(r'nan', ' ', regex=True)
    df = df.replace(r'\r\n\r\n', ' ', regex=True)
    df = df.replace(r'\t', ' ', regex=True)
    df = df.replace(r'\r\n', ' ', regex=True)
    df = df.replace(r'•', ' ', regex=True)
    df = df.replace(r';\r\n', ' ', regex=True)
    df = df.replace(r';', ' ', regex=True)
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/dim_custom_fields.csv', encoding='utf-8-sig', index=False,
              sep=';')
    df['idDatasource'] = pd.to_numeric(df['idDatasource'], errors='coerce')
    df.dropna(subset=['idDatasource'], inplace=True)
    df['idDatasource'] = df['idDatasource'].astype(int)
    dataaux.append(df['idDatasource'].tolist())
    dataaux1.append(dataaux[0])
    for c in range(len(dataaux1[0])):
        page8 = (str(dataaux1[0][c]))
        file = 'https://api.pandape.com.br/v2/data-sources/Items?IdDatasource='+page8+'&Page=1&PageSize=1000'
        try:
            resp8 = requests.get('https://api.pandape.com.br/v2/data-sources/Items?IdDatasource='+page8+'&Page=1&PageSize=1000', auth=BearerAuth(token)).json()
            dataaux2.append(resp8['items'])
        except:
            print("erro")
        with open("D:/pythonDSA/Security/python/source/log" + "/dim_data_sources" + ".json", "w") as jsonFile:
            json.dump(dataaux2, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
    for c in range(len(dataaux2)):
        for d in range(len(dataaux2[c])):
            dataaux3.append(dataaux2[c][d])
    df = pd.DataFrame.from_records(dataaux3).drop_duplicates()
    df.to_csv(r'D:/pythonDSA/Security/python/source/log/dim_data_sources.csv', encoding='utf-8-sig', index=False,
              sep=';')
    print("Finalizando data_source")


'''#Saving file to local
with open("sharepointfile.json", 'wb') as output_file:
    output_file.write(file_response.content)'''

########## MAIN

#sys.stdout = open('D:/pythonDSA/Security/python/source/log/log.txt', 'w')



data = get_requests()
with ThreadPoolExecutor(max_workers=METER_WORKERS) as executor:
    executor.submit(get_vancacies())
    executor.submit(get_id_requests(data))
    executor.submit(get_id_vacancies())
    id_users()
    executor.submit(get_id_vacancies_all(data))
    custom_fields()
    data_source()

#sys.stdout.close()