# TP4_HADOOP

The goal of this TP is to create a "social network" using HBase as a Database.

Row ID: name of the person
CFs: info, friends
in info: gender, age, address, tel, etc.
in friends:
- BFF: name of the Best Friend (row ID)
- others: array of other name (each name is a row ID)
