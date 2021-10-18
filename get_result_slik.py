"""
--===============================================================================================================================================================================
--||Author		: Henro T																																				|
--||Create date	: 08 April 2021																																					|
--||Description	: <PR/2021/MAR/PMOB/001>																															    |
--|| Version    : v1.0.20210408																																					|
--|| History	:																																								|
--||----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
--|| Date                 | Type    | Version        | Name                              | Description                                                                                      |
--||----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
--|| 08 April 2021        | Create  | v1.0.20210408  | Henro T                           | PR/2021/MAR/PMOB/001                                                                  |
--===============================================================================================================================================================================

"""

import requests
import json
from datetime import datetime
import time
import sys, os, traceback
from multiprocessing import Process
from config import connection as cnxn_ws, settings, writeLog, clear_error


api_key = settings['api_key']
api_url = settings['base_url']

cursor_ws = cnxn_ws.cursor()

log_cbas_id = ''
log_last_id_resp_h = 0
log_error = ''

def getResult():
    global log_last_id_resp_h
    global log_error
    
    q_get_data_customer = """
                            SELECT TOP 1 REFF_NUMBER,APP_NO, LOB,PRODUCT,CUST_TYPE,COMPANY,KTP,
                            NAME1,GENDER,POB,CAST(DOB AS date),NPWP,MOTHER_NAME,[ADDRESS],CITY,POSTAL,
                            BATCH,CbasID,DOB, T_SLIK_CustomerData_ID FROM T_SLIK_CustomerData WHERE is_SendCustomer = 1 AND is_GetResult = 0 ORDER BY APP_NO, T_SLIK_CustomerData_ID
                        """
    data_customer = cursor_ws.execute(q_get_data_customer)
    data_customer = cursor_ws.fetchall()

    if not data_customer:
        return True

    q_get_result = """
                SELECT GS_VALUE,
                CASE 
                    WHEN (SELECT MAX(Sequence) FROM [WISE_STAGING].[dbo].T_SLIK_GetResult_ResponseH) IS NULL
                    THEN 0 
                    ELSE (SELECT MAX(CAST([Sequence] AS datetime)) FROM [WISE_STAGING].[dbo].T_SLIK_GetResult_ResponseH)
                END AS SEQ	
                FROM CONFINS.dbo.GENERAL_SETTING 
                WHERE GS_CODE = 'SLIKSendCustomerLOB'
                """
    q_get_result = cursor_ws.execute(q_get_result)
    q_get_result = cursor_ws.fetchone()
    data_lob = q_get_result and q_get_result[0]
    data_sequence = q_get_result and q_get_result[1]
    data_sequence = data_sequence.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    params = (data_lob, data_sequence)
    data_to_send = {
        "LOB": data_lob,
        "Sequence": data_sequence
    }
    
    ############
    # start INSERT REQUEST
    ############    

    q_insert_req = """
        INSERT INTO T_SLIK_GetResult_Request (LOB,[Sequence],Request_DT)
        VALUES ('%s', '%s', GETDATE())
    """ %(params)
    
    cursor_ws.execute(q_insert_req)
    cnxn_ws.commit()
    
    q_last_id = "select @@IDENTITY"
    last_id = cursor_ws.execute(q_last_id)
    last_id = cursor_ws.fetchone()
    last_id = last_id[0]
    ############
    # end INSERT REQUEST
    ############   

    data_json = json.dumps(data_to_send)
    headers = {"Content-Type": "application/json", "API-KEY": api_key}
    response = None
    try:
        response = requests.post(api_url+'GetResult', headers=headers, data=data_json)
        response = response.json()
        # print (response)
        if not response:
            raise Exception(f"error due to response returns nothing")
    except Exception as err:
        print(f"ini errornya >>> {err}")
    if response['Flag'] == '1':    
        
        #############
        ### start UPDATE T_SLIK_CustomerData
        #############    
        q_update_log = ''
        try:
            def pisah(cbids):
                res = {}
                i = 0
                item = False
                while not item:
                    res[i]= cbids[:100]
                    del cbids[:100]
                    if not len(cbids):
                        item = True
                    i += 1
                return res
            
            all_cbas_ids = [customer.get('CbasId') for customer in response.get('Customers')]
            ids_to_update = pisah(all_cbas_ids)
            #### {0: [1, 2, 3, 4, 5], 1: [6, 7, 8, 9]} 
            for id in ids_to_update:
                final_ids = ids_to_update[id]
                questionMarks = ','.join(['?' for i in final_ids])
                q_update = f"""UPDATE T_SLIK_CustomerData SET is_GetResult = 1 , USR_UPD='JOB', DTM_UPD = GETDATE() 
                    WHERE CbasID in ({questionMarks})"""
                    
                cursor_ws.execute(q_update, (tuple(final_ids)))
                cnxn_ws.commit()
                q_update_log = q_update
            # if cbas_ids:
            #     q_update = """UPDATE T_SLIK_CustomerData SET is_GetResult = 1 
            #         WHERE CbasID in %s"""%(str(tuple(cbas_ids)))
            #     q_update_log = q_update 
                # cursor_ws.execute(q_update)
                # cnxn_ws.commit()


        except Exception as e:
            localtime = time.asctime( time.localtime(time.time()) )
            today = datetime.today().strftime('%Y%m%d')
            # file_name = 'error_log_%s.txt'%(today)
            # file_obj = open('log\\' + file_name, "a+")
            # file_obj.write("Error saat update T_SLIK_CustomerData (try dalam try)" + " ")
            # file_obj.write(str(localtime) + " ")
            # file_obj.write(str(e) + "\n")
            # file_obj.close()
            string = f"Error saat update T_SLIK_CustomerData (try dalam try) {q_update_log} {str(localtime)} {str(e)} "
            writeLog(keySetting="errorLogGetResult", string=string)
            log_error = e
        
        #############
        ### end UPDATE T_SLIK_CustomerData
        #############    

        #############
        ### start insert response_H
        #############
        
        customer_data = str(response['Customers']).replace("'", '"')
        response_result = '' # 'ini digunakan untuk log error diambil dari error handle, di isi jika is_GetResult tidak terupdate jadi 1 '
    
        if not log_error:
            params_H = (int(last_id),response['Sequence'],response['Flag'] , response['ErrorMsg'], customer_data, response_result )
            q_response_H = """ INSERT INTO T_SLIK_GetResult_ResponseH (Get_Result_Request_ID,[Sequence],Flag,ErrorMsg,Response_Data,Response_result,Request_DT )
                    VALUES (%d, '%s','%s', '%s','%s', '%s',GETDATE())
            """%(params_H)
            
        else:
            params_H = (int(last_id),response['Flag'] , response['ErrorMsg'], customer_data, response_result )
            q_response_H = """ INSERT INTO T_SLIK_GetResult_ResponseH (Get_Result_Request_ID,Flag,ErrorMsg,Response_Data,Response_result,Request_DT )
                    VALUES (%d,'%s', '%s','%s', '%s',GETDATE())
            """%(params_H)
        
        cursor_ws.execute(q_response_H)
        cnxn_ws.commit() 
        
        q_last_id_resp_h = "select @@IDENTITY"
        last_id_resp_h = cursor_ws.execute(q_last_id_resp_h)
        last_id_resp_h = cursor_ws.fetchone()
        log_last_id_resp_h = last_id_resp_h[0]
        
        #############
        ### end insert response_H
        #############

        q_detail = ''
        numb = 0
        for customer in response.get('Customers'):
            numb += 1
            global log_cbas_id
            params_D = (int(log_last_id_resp_h), customer.get('CbasId'), customer.get('ReffNumber'), customer.get('CustType'), customer.get('Din'), customer.get('Result'))
            q_detail += """
                INSERT INTO T_SLIK_GetResult_ResponseD (Get_Result_ResponseH_ID,CbasId,ReffNumber,CustType,Din,Result,Request_DT )
                VALUES (%d, '%s', '%s', '%s', '%s', '%s', GETDATE())
            """%(params_D)
        
        if q_detail:
            cursor_ws.execute(q_detail)
            cnxn_ws.commit() 
        
    else:        
        
        #############
        ### start insert response_H
        #############
        customer_data = ''
        
        response_result = ''

        params_H = (int(last_id),response['Sequence'],response['Flag'] , response['ErrorMsg'], customer_data, response_result )
        q_response_H = """ INSERT INTO T_SLIK_GetResult_ResponseH (Get_Result_Request_ID,[Sequence],Flag,ErrorMsg,Response_Data,Response_result,Request_DT )
                VALUES (%d, '%s','%s', '%s','%s', '%s',GETDATE())
        """%(params_H)
        # print (q_response_H)
        cursor_ws.execute(q_response_H)
        cnxn_ws.commit() 

        q_last_id_resp_h = "select @@IDENTITY"
        last_id_resp_h = cursor_ws.execute(q_last_id_resp_h)
        last_id_resp_h = cursor_ws.fetchone()
        log_last_id_resp_h = last_id_resp_h[0]
        
        #############
        ### end insert response_H
        #############

    return True

def execute_get_result():
    #############
    ### start insert log start job
    #############
    start_log_query = "INSERT INTO CONFINS.DBO.LOG_JOB_PROC_WOM (JOB_NAME, PROC_NAME, DATE_PROCESSED, ERR_MESSAGE, ERR_LINE, ERR_NUMBER) VALUES ('JOB_SLIK_GET_RESULT_REQ_API', 'JOB_SLIK_GET_RESULT_REQ_API_START', GETDATE(), '', NULL, NULL)"
    cursor_ws.execute(start_log_query)
    cnxn_ws.commit()
    #############
    ### end insert log start job
    #############
    global log_error
    try:
        getResult()
    except Exception as e:
        localtime = time.asctime( time.localtime(time.time()) )
        # today = datetime.today().strftime('%Y%m%d')
        # file_name = 'error_log_%s.txt'%(today)
        # file_obj = open('log\\' + file_name, "a+")
        # file_obj.write(str(localtime) + " ")
        # file_obj.write(str(e) + "\n")
        # file_obj.close()

        string = f"{localtime} {e}"
        writeLog(keySetting="errorLogGetResult", string=string)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        log_error = (exc_type, fname, exc_tb.tb_lineno)
    finally:
        print (log_error)
    
    #############
    ### start insert log finish job
    #############
    
    if log_error:
        print ('loggggggggggggggg check error', log_error)
        log_error = str(log_error)
        log_error = log_error.replace('(', '')
        log_error = log_error.replace(')', '')
        log_error = log_error.replace('<', '')
        log_error = log_error.replace('>', '')
        log_error = log_error.replace(',', '')
        log_error = log_error.replace("'", '')
        log_error += '_' + str(log_cbas_id)
        log_error = 'ERROR ' + log_error

        start_log_query_up = "INSERT INTO CONFINS.DBO.LOG_JOB_PROC_WOM (JOB_NAME, PROC_NAME, DATE_PROCESSED, ERR_MESSAGE, ERR_LINE, ERR_NUMBER) VALUES ('JOB_SLIK_GET_RESULT_REQ_API', 'JOB_SLIK_GET_RESULT_REQ_API_END' , GETDATE(), '%s', NULL, NULL)" %(log_error,)
        cursor_ws.execute(start_log_query_up)
        cnxn_ws.commit()
        
        q_response_H_up = """ UPDATE T_SLIK_GetResult_ResponseH SET Response_result = '%s'
                WHERE Get_Result_ResponseH_ID = %d
                """%(log_error, log_last_id_resp_h)

        cursor_ws.execute(q_response_H_up)
        cnxn_ws.commit()
        
    else:
        start_log_query = "INSERT INTO CONFINS.DBO.LOG_JOB_PROC_WOM (JOB_NAME, PROC_NAME, DATE_PROCESSED, ERR_MESSAGE, ERR_LINE, ERR_NUMBER) VALUES ('JOB_SLIK_GET_RESULT_REQ_API', 'JOB_SLIK_GET_RESULT_REQ_API_END', GETDATE(), '', NULL, NULL)"
        cursor_ws.execute(start_log_query)
        cnxn_ws.commit()

    #############
    ### end insert log finish job
    #############

if __name__ == '__main__':
    clear_error(keySetting="errorLogGetResult")
    execute_get_result()