import configparser

# 创建一个新的配置文件
config = configparser.ConfigParser()

# 添加数据库的配置信息
config['DATABASE'] = {
    'ENGINE': 'oracle+cx_oracle://system:123888aBc@localhost:1521/ORCL'
}

# 添加远程服务器的配置信息
config['REMOTE_SERVER'] = {
    'HOST': '192.168.241.130',
    'USERNAME': 'python',
    'PASSWORD': 'chuanzhi',
    'PATH': '/home/python/FAL/'
}

# 将配置信息写入文件
with open('config.ini', 'w') as configfile:
    config.write(configfile)


# [DATABASE]
# ENGINE = oracle+cx_oracle://system:123888aBc@localhost:1521/ORCL

# [PATH]
# FOLDER_PATH = C:\\Users\\45434\\Desktop\\FAL

# ens33: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
#         inet 192.168.241.130  netmask 255.255.255.0  broadcast 192.168.241.255
#         inet6 fe80::3dfe:c08d:5b44:b1dd  prefixlen 64  scopeid 0x20<link>
#         ether 00:0c:29:96:d6:69  txqueuelen 1000  (Ethernet)
#         RX packets 21319  bytes 1529118 (1.4 MiB)
#         RX errors 0  dropped 0  overruns 0  frame 0
#         TX packets 10842  bytes 1008199 (984.5 KiB)
#         TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
