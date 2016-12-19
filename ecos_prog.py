# -*- coding: utf-8 -*-

import urllib
import csv
import time
import xml.etree.ElementTree as etree
import os
import pandas as pd
import math


"""
전체적인 실행 개념
1. 프로그램은 Extract_csv 함수만 사용합니다.
    그외 함수들은 모두 Extract_csv 안에서 사용하는 함수들입니다.

2. 추출될 csv의 데이터의 시작날짜와 끝날짜는 Extract_csv 함수의 파라미터로 제어하며,  시작날짜는 fromdate, 끝날짜는 todate 입니다.
fromdate와 todate가 YYYY-MM-DD 형식인지 검사를 우선 진행합니다.
그다음 ecos_log 파일 작성을 위해 프로그램은 with ~ logfp 안에서 돌아갑니다.

대략적인 프로그램의 순서입니다.
1. ecos_var_desc 를 불러와 df_varDesc 이라는 dataframe 변수에 집어넣는다.

2. Create_csv_Form 함수를 사용하여 ecos_result.csv의 폼을 만들고, 만들어진 폼을 df_Result 이라는 dataframe 변수에 read_csv 함수를 사용하여 집어 넣는다.  이때, low_memory=False를 하지 않으면 데이터 양이 많을 경우에 데이터가 손실되어나오기 때문에 *꼭* 집어넣는다.

3. FileManger의 CODE 컬럼을 리스트화 시킨 statCodelists 변수의 element 들을 순서대로 반복문을 이용하여 불러오는 방식을 사용하여 ecos_result.csv의 컬럼을 하나씩 채워나간다.

4. Load_period 함수를 사용하여 현재 통계코드의 UPDATE_CYCLE 을 알아낸다.
UPDATE_CYCLE의 종류는 YY,QQ,MM,DD가 있다.

5. statCode,itemCoe,restItemCode를 초기화시킵니다.

6. Find_url 함수를 사용하여 uuu에 프로그램에서 사용할 url을 찾습니다.

7. Find_nowdate 함수를 사용하여 todate를 현재 코드의 UPDATE_CYCLE에 맞게 변환시킵니다. 예) 20160811 을 MM에 맞춰 변환하면 201608 이 됩니다.

8. uuu의 주소에 있는 내용을 (코드).txt에 저장한 후, 
etree로 이름지은(as) xml.etree.ElementTree 라이브러리를 사용하여 
(코드).txt의 내용을 파싱 후 가장 윗계층(root)를 찾아 root 변수에 저장합니다.

9. findall 함수를 사용하여 모든 TIME과 DATA_VALUE 값을 zip함수로 묶은 뒤 반복문을 사용하여 (filename).txt 에 저장합니다.

10. df_Result에 각 itemCount 를 이름으로 하는 새로운 컬럼을 추가한 뒤, 에러체킹을 한다.

11. df_varDesc 에 변수이름, code, UPDATE_CYCLE, 에러코드, 업데이트된 날짜를 추가합니다.

12. Count, STAT_CODE, ITEM_CODE, STAT_NAME, ITEM_NAME1, ITEM_NAME2, ITEM_NAME3, UNIT_NAME, UPDATE_CYCLE 을 root에서 찾아내어 writerow를 이용, (statCode_itemCode[1]).csv 에 작성한다.

13. csv_in은 (filename).txt의 내용을 읽어들여 반복문을 사용하여 모든 (TIME,DATA_VALUE)를 writerow를 이용, (statCode_itemCode[1]).csv 에 작성한다.

14.  (statCode_itemCode[1]).csv의 내용을 df_Current 라는 dataframe 변수에 저장한다.

15. 이중반복문을 이용, df_Result 의 날짜부분과 df_Current의 날짜부분이 일치하면, df_Current의 DATA_VALUE 값을 df_Result 의 DATA_VALUE에 넣는다.

16. deletFileList 에 삭제할 파일들의 이름을 추가한다.

17. 4~14의 내용은 3에서도 말했듯이 반복문입니다. statCoelists가 끝날때까지 반복됩니다.

18. df_varDesc은 ecos_var_desc.csv로 추출합니다.

19. df_Result 이라는 dataframe은 sort_values를 이용하여 내림차순 정렬하여 가장 최신의 날짜가 가장 위로 올라오게 합니다.

20. 정렬된 데이터는 ecos_result.csv 로 추출합니다.

21. 끝입니다.


"""

def Load_period(code):  # ECOS의 서비스 통계목록을 이용하여 해당 통계의 UPDATE_CYCLE을 리턴하는 함수
    url_1="http://ecos.bok.or.kr/api/StatisticTableList/"
    auth_key="MYQYZU1H7DB8MC8L55KY"
    url_2="/xml/kr/1/"
    end_page=2
    uuu=url_1+auth_key+url_2+str(end_page)+"/"+code

    # uuu는 ECOS의 서비스통계목록을 조회하는 url 입니다.
    
    while True: # while True를 이용하여 url loading 을 성공할 때까지 무한 반복
        url=urllib.urlopen(uuu) # uuu의 내용을 url 변수에 저장
        if url!=None: # 성공하면
            break # break
        print("url loading failed.. retry") #실패 할 경우 실패했다고 출력 후 while True 에 의해 다시 반복
        
    with open(code[0:7]+"_load_period.txt",'w') as fp: # xml의 parsing을 하기 위해 url의 내용을 txt에 저장
        fp.write(url.read())
    fp.close()
    
    tree=etree.parse(code[0:7]+"_load_period.txt") # xml.etree.ElementTree 라이브러리를 이용하여 tree에 파싱한 내용 저장
    root=tree.getroot() # getroot는 xml을 파싱한 후 계층의 가장 위, root가 되는 부분을 얻는 함수.
    
    try:
        period=root.find("row/CYCLE") # find 함수는 파라미터 안의 경로에서 가장 먼저 찾아지는 값을 반환.
                                        # 즉, row/CYCLE 의 가장 첫번째 값이 period가 된다.
    except:
        print("UPDATE_CYCLE loading failed.. retry")
    
        
        
    print(code,str(period.text)) #코드 출력
    return str(period.text) # period 를 반환.

def Create_csv_Form(fromdate,todate): # Create_csv_Form 함수는 결과물의 Form 이 될 csv 파일을 fromdate에서 todate까지
                                    # 폼을 만들어주는 함수.
                                    
    with open('ecos_result.csv','wb') as form_f: # ecos_result.csv 파일을 엽니다. 
            form = csv.writer(form_f,delimiter=',')
            form.writerow(['Count'])    
            form.writerow(['STAT_CODE'])
            form.writerow(['ITEM_CODE'])
            form.writerow(['STAT_NAME'])
            form.writerow(['ITEM_NAME1'])
            form.writerow(['ITEM_NAME2'])
            form.writerow(['ITEM_NAME3'])
            form.writerow(['UNIT_NAME'])
            form.writerow(['UPDATE_CYCLE'])
            for a in range(fromdate,todate+1):
                y = str(a)[0:4] #fromdate, todate는 YYYY-MM-DD의 8자리의 문자이므로, y,m,d를 지정해줍니다.
                m = str(a)[4:6]
                d = str(a)[6:8]
                date_form=y+'-'+m+'-'+d
                
                # 이제 m(월)에 맞게 d(일)이 입력됩니다.
                if (m == '01' or m == '03' or m=='05' or m=='07' or m=='08' or m=='10' or m=='12'):
                    if(int(d) > 0 and int(d) < 32):
                        form.writerow([date_form])
                        continue
                elif (m=='04' or m=='06' or m=='09' or m=='11'):
                    if(int(d) > 0 and int(d) < 31):
                        form.writerow([date_form])
                        continue
                elif (m =='02' and int(y)%4==0): # 윤년일경우 2월은 29일까지 입력.
                    if(int(d) > 0 and int(d) < 30):
                        form.writerow([date_form])
                        continue
                elif (m=='02' and int(y)%4!=0): # 윤년이 아니면 2월은 28일까지 입력
                    if(int(d) > 0 and int(d) < 29):
                        form.writerow([date_form])
                        continue
                else: continue

def Find_nowdate(period,date): # Find_nowdate 함수는 파라미터의 date를 period에 맞게 변환시켜줍니다.
    nowdate=''
    if period=='YY': # period 가 YY면  (년단위)
        nowdate=str(date)[:4] #nowdate는 YYYY 형식으로 변환.
        
    elif period=='QQ': # period 가 QQ면 (분기단위)
        nowdate=str(date)[0:4]+str(int(str(date)[4:6])//3) #nowdate는 YYYYQ 형식으로 변환.
        
    elif period=='MM': # period 가 MM이면 (월단위)
        nowdate=str(date)[:6] # YYYYMM 형식으로 
        
    elif period=='DD': # period 가 DD이면 (일단위)
        nowdate=str(date) # YYYY-MM-DD 형식으로
        
    return nowdate # period 에 맞게 변환된 nowdate를 반환.


def Find_url(c,fromdate,todate,period): # Find_url 함수는 c, fromdate, todate, period를 이용하여 ECOS 의 통계조회조건 설정 서비스를
                                        # 이용하여 c(통계코드)의 데이터를 fromdate 부터 todate 까지 불러옵니다.
    url_1="http://ecos.bok.or.kr/api/StatisticSearch/"
    auth_key="MYQYZU1H7DB8MC8L55KY"
    url_2="/xml/kr/1/"
    end_page=2
    statCode=c.split("/")[0] # c는 통계코드/아이템코드1/아이템코드2..이런식으로 이루어져있으므로 /단위로 split 한것의 가장 처음이 통계코드가 됩니다.
    
    itemCode=[]
    itemCode = [a for a in c.split("/")] #itemCode는 /단위로 split 한 것을 list 형식으로 저장해놓은것이며, 0은 통계코드, 1은 아이템코드1, 2는 아이템코드2...가 됩니다.
    restItemCode = [] #restitemCode는 statCode를 제외한 나머지 아이템코드들의 합입니다.
    restItemCode = c[8:]   
    
            
    url_3=''.join(["/",Find_nowdate(period,fromdate),"/"]) #url_3는 fromdate를 Find_nowdate 함수를 사용하여 period 에 맞게 변환하여 사용합니다.
    nowdate=Find_nowdate(period,todate) #nowdate는 todate를 Find_nowdate 함수를 사용하여 period에 맞게 변환한 값입니다.

    uuu=url_1+auth_key+url_2+str(end_page)+"/"+statCode+"/"+period\
        +url_3+nowdate+"/"+restItemCode

    while True: # while True를 이용하여 url loading 을 성공할 때까지 무한 반복
        url=urllib.urlopen(uuu)
        if url!=None:
            print("url loading success..")
            break                    
        print("url loading failed.. retry")
        
    
    with open(statCode+".txt",'w') as fp:  # xml의 parsing을 하기 위해 url의 내용을 txt에 저장
        fp.write(url.read())
    fp.close()

    tree=etree.parse(statCode+".txt") # xml.etree.ElementTree 라이브러리를 이용하여 tree에 파싱한 내용 저장
    root=tree.getroot() # getroot는 xml을 파싱한 후 계층의 가장 위, root가 되는 부분을 얻는 함수
    for i in {'list_total_count'}: # list_total_count는 불러와진 데이터의 갯수를 의미하며, 이는 url 에서 사용됩니다.
        for j in root.iter(i):
            end_page=j.text

    uuu=url_1+auth_key+url_2+str(end_page)+"/"+statCode+"/"+period\
        +url_3+nowdate+"/"+restItemCode
    #최종적인 url은 코드의 내용을 fromdate 부터 todate까지의 모든 데이터를 불러오는 url 이 됩니다.
    
    return uuu


def Extract_csv(fromdate,todate): # Extract_csv 함수를 사용하여 결과물 (csv)을 사용자가 지정한 날짜에 맞게 추출합니다.
    
    if len(fromdate)!=10 or len(todate)!=10: # fromdate와 todate가 YYYY-MM-DD 형식이 아니면 함수 사용을 못하므로,
        print "date input Error"                     # YYYY-MM-DD 형식이 맞는지 검사합니다.
        exit()

    fromdate=int(fromdate.replace('-',''))
    todate=int(todate.replace('-',''))
    
    with open("ecos_log.txt",'w') as logfp: # 로그작성을 위한 파일포인터를 엽니다. 모든 프로그램은 이 with 문 안에서 실행되며, 프로그램
                                        # 진행에 맞춰 ecos_log가 작성됩니다.
        df_varDesc = pd.read_csv('ecos_var_desc.csv',low_memory=False,index_col='Count') #ecos_var_desc 파일을 df_varDesc이라는 dataframe에 저장합니다.
        print df_varDesc.loc[:,'CODE'] # File_Manager에 적혀있는 CODE를 모두 출력합니다.
        statCodelists=df_varDesc['CODE'].values.tolist() # statCodelists는 사용할 통계코드를 list 형식으로 저장합니다.
        
        
        Create_csv_Form(fromdate,todate) # Create_csv_Form 함수를 이용하여 ecos_result.csv를 생성합니다.
        df_Result = pd.read_csv('ecos_result.csv',low_memory=False) #df_Result은 ecos_result.csv를 읽어들여
        
        itemCount=0 # Column의 이름을 제어할 itemCount를 초기화합니다.
        
        for c in statCodelists: # statCodelists의 element 들을 c로 이름짓고 반복문을 돌립니다.
            
            logfp.write("\n--------------------- "+c+" Start---------------------------\n")
            print "---------------------",c,"Start---------------------------"
            
            itemCount = itemCount + 1 #itemCount는 1씩 더합니다.
            period=Load_period(c) # Load_period 함수를 이용하여 period를 구합니다.
            
            statCode=c.split("/")[0] # c는 통계코드/아이템코드1/아이템코드2..이런식으로 이루어져있으므로 /단위로 split 한것의 가장 처음이 통계코드가 됩니다.
            
            itemCode=[]  
            itemCode = [a for a in c.split("/")] #itemCode는 /단위로 split 한 것을 list 형식으로 저장해놓은것이며, 0은 통계코드, 1은 아이템코드1, 2는 아이템코드2...가 됩니다.
            restItemCode = [] 
            restItemCode = c[8:]#restitemCode는 statCode를 제외한 나머지 아이템코드들의 합입니다.
            uuu=Find_url(c,fromdate,todate,period) # Find_url 함수를 사용하여 ECOS의 통계 조회 조건 설정 서비스를 이용하기 위한 url을 구합니다.
            nowdate=Find_nowdate(period,todate) #Find_nowdate 함수를 사용하여 todate를 period에 맞게 변환합니다.

            
            while True: # while True를 이용하여 url loading 을 성공할 때까지 무한 반복
                try :
                    url=urllib.urlopen(uuu)
                    break
                except IOError:
                    print("url loading failed.. retry")
                
                
            fp=open(statCode+".txt",'w') # xml의 parsing을 하기 위해 url의 내용을 txt에 저장
            fp.write(url.read())
            fp.close()

            try:
                root=etree.parse(statCode+".txt").getroot()  # xml.etree.ElementTree 라이브러리를 이용하여 tree에 파싱한 내용 저장
            except:
                print("getroot failed")
                logfp.write("getroot failed") # root를 얻는데 실패하면 ecos_log에 실패했다고 작성 한 뒤, 다음 통계코드로 넘어갑니다.
                continue
                    
            print "\n",c.replace("/","_"),".txt extract Start" # 통계코드에서 조회된 데이터를 담을 txt를 만듭니다.
            logfp.write(c.replace("/","_")+".txt extract Start\n") 
            
            filename=c.replace("/","_")+".txt" # filename에는 /가 사용이 안되므로, _로 변환해줍니다.
            
            with open(filename, 'wb') as f:
                for a in zip(root.findall("row/TIME"),\
                             root.findall("row/DATA_VALUE")): # findall 함수는 row/TIME의 모든 데이터를 찾아줍니다. zip함수를 사용하여 (TIME,DATA_VALUE) 형식으로 묶어줍니다.
                    if a==None: # a가 없을경우
                        print "  " + itemCode[1] +" is Unicode Error" #에러문을 출력하고 다음 코드로 넘어갑니다.
                        continue
                    data="\t".join([str(x.text) for x in a]) #a는 TIME,DATA_VALUE가 묶인 형태이고, 반복문을 통해 TIME \t DATA_VALUE형식으로 변환한 data 변수를 만듭니다.
                    data=data.encode('euc-kr') # 한글 출력을 위해 euc-kr로 인코딩해줍니다.
                    f.write(data) # data를 filename.txt에 작성합니다.
                    f.write("\n")
            f.close()
            
            print itemCount
            logfp.write(filename +" extract End\n")
            print filename,"extract End\n"

            errorCode = None # 무슨 에러가 있는지 알려줄 errorCode를 초기화합니다.

            df_Result[str(itemCount)]=-8 # df_Result 데이터프레임에 str(itemCount) 이름의 새로운 컬럼을 생성하고, 모든 데이터를 -8로 초기화합니다.
            
            if root.find("row/STAT_NAME")==None: # 데이터를 불러들이는데 실패하면 row/STAT_NAME이 없을것이므로, row/STAT_NAME이 없다면,
                errorCode=-9 #에러코드는 -9가 되고 이를 ecos_var_desc에 출력합니다.
                df_Result[str(itemCount)]=-9
                df_varDesc.loc[itemCount] = None,c.encode('euc-kr'),"MYQYZU1H7DB8MC8L55KY","ecos_result.csv",None,None,None,nowdate.encode('euc-kr'),errorCode
                print itemCount,c,"Code Error. Please check statCode"
                logfp.write("**"+str(itemCount)+" "+c+"Code Error. Please check statCode\n\n") # 실패할경우 실패했다고 ecos_log 를 작성해줍니다.
                deleteFileList.append(filename)
                continue
            
            statName=root.find("row/STAT_NAME").text.encode('euc-kr') #statName은 통계코드로 조회한 통계의 이름입니다.
            itemName=root.find("row/ITEM_NAME1").text.encode('euc-kr') #itemName은 통계코드로 조회한 통계 item1의 이름입니다.
            itemName2=root.find("row/ITEM_NAME2").text.encode('euc-kr') #itemName2은 통계코드로 조회한 통계 item2의 이름입니다.
            itemName3=root.find("row/ITEM_NAME3").text.encode('euc-kr') #itemName3은 통계코드로 조회한 통계 item3의 이름입니다.
            
            
            if root.find("row/UNIT_NAME").text==None: # UNIT_NAME이 없을 경우, UNIT_NAME은 없는 것(None)으로 직접 지정해줍니다.
                unitName=None
            else:
                unitName=root.find("row/UNIT_NAME").text.encode('euc-kr')
            
            print "------",statName,itemName,itemName2,"\n"
            logfp.write("------"+statName+itemName+itemName2+"\n")
            
            print "\nurl :\n",uuu.encode('euc-kr')
            logfp.write("\nurl :\n"+uuu.encode('euc-kr')+"\n")
            
            print statCode," ",restItemCode,".csv write Start"
            logfp.write(statCode+" "+restItemCode+".csv write Start\n")

            
            csv_in = csv.reader(open(filename,'rb'),delimiter='\t') # filename.txt를 reader 함수를 이용해 \t를 기준으로 셀을 구분하여 csv_in에 저장합니다.
            
            with open(statCode+"_"+itemCode[1]+'.csv','wb') as csv_f: # 데이터의 내용을 df_Current에 저장하기 위해 내용을 csv로 작성합니다.
                csv_out = csv.writer(csv_f,delimiter=',')
                csv_out.writerow(['Count',itemCount])
                csv_out.writerow(['STAT_CODE',statCode.encode('euc-kr')])
                csv_out.writerow(['ITEM_CODE',itemCode[1].encode('euc-kr')])
                csv_out.writerow(['STAT_NAME',statName])
                csv_out.writerow(['ITEM_NAME1',itemName])
                csv_out.writerow(['ITEM_NAME2',itemName2])
                csv_out.writerow(['ITEM_NAME3',itemName3])
                csv_out.writerow(['UNIT_NAME',unitName])
                csv_out.writerow(['UPDATE_CYCLE',period.encode('euc-kr')])

                for line in csv_in:
                    csv_out.writerow([x for  x in line])
                    
            print statCode," ",restItemCode,".csv write End\n"
            logfp.write(statCode+" "+restItemCode+".csv write End\n")
            
            print "df_Current read_csv Start"
            df_Current = pd.read_csv(statCode+"_"+itemCode[1]+'.csv',low_memory=False) # 위에서 작성한 csv를 사용하여 df_Current에 데이터 내용을 저장합니다.
            print "df_Current read_csv End\n"

            max_value=float(df_Current[str(itemCount)][8]) # ecos_var_desc에 max값과 min값을 작성하기 위해 max_value,min_value를 구합니다.
            min_value=float(df_Current[str(itemCount)][8])
            
            for i in range(8,len(df_Current.Count)): # max_value를 구하는 반복문
                if float(df_Current[str(itemCount)][i]) > max_value:
                    max_value = float(df_Current[str(itemCount)][i])

            for i in range(8,len(df_Current.Count)): # min_value를 구하는 반복문
                if float(df_Current[str(itemCount)][i]) < min_value:
                    min_value = float(df_Current[str(itemCount)][i])

            df_varDesc.loc[itemCount] = statName+"_"+itemName+"_"+itemName2,c.encode('euc-kr'),"MYQYZU1H7DB8MC8L55KY","ecos_result.csv",\
                                   period.encode('euc-kr'),min_value,max_value,nowdate.encode('euc-kr'),errorCode
            # df_varDesc 에 ecos_var_desc에 양식에 맞게 통계를 불러올때 사용한 데이터들을 입력합니다.
            

            # df_Current와 df_Result 의 날짜가 같으면 df_Current의 값을 df_Result으로 옮겨주는 작업.
            # df_Current의 기준이 되는 날짜는 period 에 따라 다른형식(예: 20162, 201608...)이고,
            # df_Result의 Count는 YYYY-MM-DD 형식이므로 이를 맞춰주는 작업입니다.
            print "df_Result,df_Current join Start"
            
            curIndex=0 # curIndex는 df_Current의 반복문 제어에 사용됩니다.
            isData = []

            
            for cur in df_Current.Count:  #df_Current의 Count는 df_Current에서 사용되는 날짜입니다. period에 따라 다릅니다.
                outIndex = 0 #outIndex는 df_Result의 반복문 제어에 사용됩니다. 
                for out in df_Result.Count: #df_Result의 Count는 df_Result에서 사용되는 날짜입니다. 무조건 YYYY-MM-DD 형식입니다.
                    if curIndex < 8: # df_Current의 8번째 row까지는 period에 상관없이 모두 같으므로 8까지일경우엔 바로 df_Result으로 옮겨줍니다.
                        if out==cur:
                            df_Result[str(itemCount)][outIndex] = df_Current[str(itemCount)][curIndex]
                            
                    elif curIndex >= 8: # 8번째 row부터는 period에 맞춰서 값을 넣어줍니다.
                        if period=='DD': 
                            if int(out.replace('-',''))==cur:
                                df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                            
                                #0123-56-89
                        elif period=='MM':
                            if out.replace('-','')[0:6]==cur[0:6]:
                                df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                            
                                
                        elif period=='YY':
                            if out.replace('-','')[0:4]==cur[0:4]:
                                df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                            
                                
                        elif period=='QQ': # period가 QQ 일 경우에는, 1분기는 1,2,3월. 2분기는 4,5,6월. 3분기는 7,8,9월. 4분기는 10,11,12월에 맞게 데이터를 옮겨줍니다.
                            if out.replace('-','')[0:4]==cur[0:4]:
                                if cur[4:5]=='1':
                                    if out.replace('-','')[4:6]=='01' or out.replace('-','')[4:6]=='02' or out.replace('-','')[4:6]=='03':
                                        df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                                        
                                elif cur[4:5]=='2':
                                    if out.replace('-','')[4:6]=='04' or out.replace('-','')[4:6]=='05' or out.replace('-','')[4:6]=='06':
                                        df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                                        
                                elif cur[4:5]=='3':
                                    if out.replace('-','')[4:6]=='07' or out.replace('-','')[4:6]=='08' or out.replace('-','')[4:6]=='09':
                                        df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                                        
                                elif cur[4:5]=='4':
                                    if out.replace('-','')[4:6]=='10' or out.replace('-','')[4:6]=='11' or out.replace('-','')[4:6]=='12':
                                        df_Result[str(itemCount)][outIndex]=df_Current[str(itemCount)][curIndex]
                    outIndex = outIndex + 1
                curIndex = curIndex + 1

            #deleteFileList는 사용하고 난 뒤의 csv,txt 파일들을 삭제할 파일 목록입니다.
            deleteFileList.append(filename)
            deleteFileList.append(statCode+"_"+itemCode[1]+'.csv')
            deleteFileList.append(statCode+"_load_period.txt")
            deleteFileList.append(statCode+'.txt')
            print "df_Result,df_Current join End\n\n"
            
            print statCode," success\n"
            #-------------여기까지 statCodelists를 도는 반복문입니다.-------------------------
            

        df_varDesc.to_csv('ecos_var_desc.csv') # 사용한 데이터를 정리한 df_varDesc 데이터프레임을 ecos_var_desc.csv로 추출합니다.
        
        df_Result.sort_values(by=['Count'],ascending=[False],inplace=True) # df_Result을 내림차순으로 정렬하여 가장 최신의 날짜가 위로 오게 합니다.
        df_Result.to_csv('ecos_result.csv',index=False) # 정렬된 df_Result을 ecos_result.csv로 추출합니다.
        
        logfp.write("\n\nDataMining Process Success\n") # 데이터를 모으는 작업이 모두 끝났다고 ecos_log에 작성합니다.
    logfp.close()
    print('Process success')


deleteFileList=[] #deleteFileList를 Extract_csv 함수를 사용하기전에 초기화합니다.

Extract_csv('1980-01-01',time.strftime("%Y-%m-%d")) # 1980년부터 오늘까지 ecos_var_desc에 있는 코드의 내용을 조회하여 csv로 추출합니다.

deleteFileList=list(set(deleteFileList)) # deleteFileList에 중복된 데이터를 없애주기 위해 set으로 변환한뒤 다시 list로 바꿔줍니다.

for x in deleteFileList:
    os.remove(x) # os.remove 함수를 사용하여 deleteFileList에 있는 파일들을 삭제합니다.

print ('All success')
