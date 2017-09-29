#-*- coding: utf-8 -*-
from wxpy import *

bot = Bot(console_qr=True, cache_path='temp/wxpy.pkl')
bot.enable_puid('temp/wxpy_puid.pkl')

groups = bot.groups()
for group in groups:
    print group.puid, group

group = groups[0]
details = group.raw
for key,val in details.items():
	print key, val

'''
members = group.members
for member in members:
    print member.puid, member

print '------------'
hmc = bot.friends().search(u'韩梦成')[0]
print hmc.puid, hmc

print hmc.raw
'''



