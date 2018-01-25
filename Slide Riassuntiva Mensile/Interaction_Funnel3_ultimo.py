#Generazione dei Funnels
import sys
import pandas as pd
import numpy as np
import pdb
import math
#Interaction channel

def Number_Eligible(DF):
	Num_Eligible=len(DF[DF['Progressivo_Follow_Up']==1])
	print('Numero Eligible Users ',Num_Eligible)

def Calcolo_M_Scambiati_Medio(DF):
	Vals=DF['Numero_messaggi_scambiati']
	Vals=Vals.dropna()
	Vals=pd.to_numeric(Vals, errors='coerce')
	Vals=Vals[Vals>4]
	print('Media messaggi scambiati:',Vals.mean()+1)

def Pulitura_DF(File_name,Skip=None):
	#pdb.set_trace()
	DF=pd.read_csv(File_name ,low_memory=False, sep=';') #apertura file csv
	Calcolo_M_Scambiati_Medio(DF)
	Number_Eligible(DF)
	DF=DF[['User_ID','Progressivo_Follow_Up','Risultato_contatto_Whatsapp','Utente_Convertito','TIPOLOGIA_UTENTE']] #prendo le righe che mi servono
	DF.User_ID=DF.User_ID.astype(str) #Trasformo in stringa tutto
	DF.Progressivo_Follow_Up=DF.Progressivo_Follow_Up.astype(str)
	DF.Risultato_contatto_Whatsapp=DF.Risultato_contatto_Whatsapp.astype(str)
	DF.Utente_Convertito=DF.Utente_Convertito.astype(str)

	DF=DF[DF.Utente_Convertito != 'Light'] #Scarto le righe che contengono light
	DF=DF[DF.Progressivo_Follow_Up != 'Spontaneo'] #Scarto le righe che hanno Spontaneo come N_Contatto

	#Mapping per inasattezze RISPOSTA
	#Infatti, la colonna Risposta contiene il SI e NO scritto in molti modi
	#Allora con map posso cambiarli tutti in NO e SI, in questo modo ho solo 2 tipi di risposta
	risposte={'SI':'SI','No':'NO','no':'NO','Sì':'SI','NO':'NO'}
	#DF['Risultato_contatto_Whatsapp']=DF['Risultato_contatto_Whatsapp'].map(risposte)
	DF['Utente_Convertito']=DF['Utente_Convertito'].map(risposte)
	risposte2={'Ha visualizzato e interagito':'SI',
				'Ha visualizzato ma non ha interagito':'NO',
				'Non ha visualizzato':'NO'}
	DF['Risultato_contatto_Whatsapp']=DF['Risultato_contatto_Whatsapp'].map(risposte2)
	
	#pdb.set_trace()
	DF=DF[DF['TIPOLOGIA_UTENTE']!='UTENTI DELAYED'] #Tolgo gli utenti delayed se ce ne fossero
	return(DF)

def Calc_Funnel(DF,Step,User_id=None):
	
	DF2=DF.copy()
	if (Step!='1'):
		DF=DF[((DF['Progressivo_Follow_Up']==Step) & (DF['User_ID'].isin(User_id)))]
		
	#STEP 1
	#INTERACTING USERS_1
	#Num di utenti che rispondono al primo contatto
	#Gli interacting users sono quelli che rispondono
	Rows_Int=np.where((DF['Progressivo_Follow_Up']==Step) & (DF['Risultato_contatto_Whatsapp']=='SI'))
	Num_Int=len(Rows_Int[0]) #Numero di interacting users

	if Num_Int==0:
		pdb.set_trace()
		print('Tutto 0')
		return()

	Denominatore_Int_users=np.where((DF['Progressivo_Follow_Up']==Step) & ((DF['Risultato_contatto_Whatsapp']=='SI') | (DF['Risultato_contatto_Whatsapp']=='NO') | (DF['Risultato_contatto_Whatsapp'].isnull())))
	Denominatore_Int_users=len(Denominatore_Int_users[0]) #Denominatore per calcolare la % di interac.users

	# % degli utenti interacting
	#pdb.set_trace()
	Ratio_Int=(Num_Int/Denominatore_Int_users)*100

	#Salvo in un dataframe a parte SOLO gli interactive users
	DF_Int=DF.iloc[Rows_Int] #Rows_Int_1 contiene gli indici di interactive users

	#CONVERTED

	#Utenti dichiarati convertiti - Declared Conversion
	Rows_Conv=np.where(DF_Int['Utente_Convertito']=='SI')
	Num_Conv=len(Rows_Conv[0])

	Denominatore_Converted=np.where((DF_Int['Utente_Convertito']=='SI') | (DF_Int['Utente_Convertito']=='NO') | (DF_Int['Utente_Convertito'].isnull()) )
	Denominatore_Converted=len(Denominatore_Converted[0])
	Ratio_Conv=(Num_Conv/Denominatore_Converted)*100

	#IN CONVERSION
	Rows_InConv=np.where((DF_Int['Utente_Convertito'].isnull())|(DF_Int['Utente_Convertito']=='NO'))
	Num_InConv=len(Rows_InConv[0])
	Denominatore_InConv=Denominatore_Converted
	Ratio_InConv=(Num_InConv/Denominatore_InConv)*100

	print('Step%s: Perc.Interacting %.2f, Perc.Converted %.2f,  Perc.InConversion %.2f' % (Step,Ratio_Int,Ratio_Conv,Ratio_InConv))
	#pdb.set_trace()
	#Ora che ho stampato i risultati per il primo step, devo passare i dati giusti per lo step2
	#Sopravvivono solo gli utenti IN CONVERSION

	#pdb.set_trace()
	Rows_InConv=(((DF2['Utente_Convertito'].isnull())) & (DF2['Progressivo_Follow_Up']==Step) & (DF2['Risultato_contatto_Whatsapp']=='SI'))
	User_ID_InConv=DF2[Rows_InConv]['User_ID']
	User_ID_InConv=list(User_ID_InConv)

	return(User_ID_InConv)


#MAIN
File_name="1_30Novembre_Ripulito_csv.csv"
DF=Pulitura_DF(File_name)
User_ID_InConv_1=Calc_Funnel(DF,'1')
User_ID_InConv_2=Calc_Funnel(DF,'2',User_ID_InConv_1)
User_ID_InConv_3=Calc_Funnel(DF,'3',User_ID_InConv_2)
User_ID_InConv_4=Calc_Funnel(DF,'4',User_ID_InConv_3)




