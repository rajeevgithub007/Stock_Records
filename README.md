# Stock_Records
#Open stock file with records
# Read the contents of file and split them line by line
# Filter the records by msg_type=8 and msg_type=12
# Create a left out join for msg_type=12 to msg_Type=8 with fields through python:
# {{"msgType_":12,"length_":25,"seqNo_":1779},"bookEntry_":{"securityId_":2,"side_":BUY,"quantity_":402,"price_":121550000,"orderId_":532424}}
# 0 {{"msgType_":8,"length_":36,"seqNo_":281},"security_":{"securityId_":4491,"umtf_":"BVCl","isin_":"IL0010849045","currency_":"GBX","mic_":"XLON","tickTableId_":32,"flags_":"{"b_":{"closingEnabled_":1,"testStock_":0,"illiquid":0,"live_":1,"aodEnabled_":1},"v_":25}}}
# with common key as security_id
# Export the resultant list of dictionary records to a tsv file 

####################################

# "securityId_":2,"side_":BUY,"quantity_":402,"price_":121550000,"isin_":"IL0010849045","currency_":"GBX", sum("side_['BUY']"),sum("side_['SALE']"),
# sum("quantity_") where side_['BUY'],sum("quantity_") where side_['SALE'],sum(∑(p*q)/∑q),max("price_") where side_['BUY'], max("price_") where side_['SALE']
####################################
# select msg12.bookEntry_["securityId_"], msg12.bookEntry_["side_"], msg12.bookEntry_["quantity_"], msg12.bookEntry_["price_"],
# msg8.security["security_id], msg8.security["isin_"],msg8.security["currency_"]
#	(
#		select bookEntry_["securityId_"], bookEntry_["side_"], bookEntry_["quantity_"], bookEntry_["price_"] from table msgType_12 
#	) msg12
 
# left outer join
 
#	(
#		select security["security_id], security["isin_"],security["currency_"] from table msgType_8
#	)msg8 ON msg12.bookEntry_["securityId_"]= msg8.security["security_id]
 