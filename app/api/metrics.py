# import requests
# import prometheus_client
# from prometheus_client import generate_latest, Info, Gauge
# from web3 import Web3, HTTPProvider

# from . import metrics_blueprint
# from ..config import config
# from ..models import Settings, db


# prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
# prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
# prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)


# def get_latest_release(name):
#     if name == 'geth':
#         url = 'https://api.github.com/repos/ethereum/go-ethereum/releases/latest'
#     elif name == 'prysm':
#         url = 'https://api.github.com/repos/prysmaticlabs/prysm/releases/latest'
#     else:
#         return False
#     data = requests.get(url).json()
#     version = data["tag_name"].split('v')[1]
#     info = { key:data[key] for key in ["name", "tag_name", "published_at"] }
#     info['version'] = version
#     return info

# def get_all_metrics():
#     w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))

#     if w3.isConnected:
#         response = {}
#         last_fullnode_block_number = w3.eth.block_number
#         response['last_fullnode_block_number'] = last_fullnode_block_number
#         response['last_fullnode_block_timestamp'] = w3.eth.get_block(w3.toHex(last_fullnode_block_number))['timestamp']
    
#         geth_version = w3.clientVersion
#         geth_version = geth_version.split('v')[1].split('-')[0]
#         response['geth_version'] = geth_version
    
#         pd = Settings.query.filter_by(name = 'last_block').first()
#         last_checked_block_number = int(pd.value)
#         response['ethereum_wallet_last_block'] = last_checked_block_number
#         block =  w3.eth.get_block(w3.toHex(last_checked_block_number))
#         response['ethereum_wallet_last_block_timestamp'] = block['timestamp']
#         response['ethereum_fullnode_status'] = 1
#         return response
#     else:
#         response['ethereum_fullnode_status'] = 0
#         return response

# geth_last_release = Info(
#     'geth_last_release',
#     'Version of the latest release from https://github.com/ethereum/go-ethereum/releases'
# )

# prysm_last_release = Info(
#     'prysm_last_release',
#     'Version of the latest release from https://github.com/prysmaticlabs/prysm/releases'
# )

# geth_last_release.info(get_latest_release('geth'))
# prysm_last_release.info(get_latest_release('prysm'))

# geth_fullnode_version = Info('geth_fullnode_version', 'Current geth version in use')
# prysm_fullnode_version = Info('prysm_fullnode_version', 'Current prysm version in use')

# ethereum_fullnode_status = Gauge('ethereum_fullnode_status', 'Connection status to ethereum fullnode')

# ethereum_fullnode_last_block = Gauge('ethereum_fullnode_last_block', 'Last block loaded to the fullnode', )
# ethereum_wallet_last_block = Gauge('ethereum_wallet_last_block', 'Last checked block ')  #.set_function(lambda: BlockScanner().get_last_seen_block_num())

# ethereum_fullnode_last_block_timestamp = Gauge('ethereum_fullnode_last_block_timestamp', 'Last block timestamp loaded to the fullnode', )
# ethereum_wallet_last_block_timestamp = Gauge('ethereum_wallet_last_block_timestamp', 'Last checked block timestamp')

# @metrics_blueprint.get("/metrics")
# def get_metrics():
#     response = get_all_metrics()
#     if response['ethereum_fullnode_status'] == 1:
#         geth_fullnode_version.info({'version': response['geth_version']})
#         #prysm_fullnode_version.info({'version':''})
#         ethereum_fullnode_last_block.set(response['last_fullnode_block_number'])
#         ethereum_fullnode_last_block_timestamp.set(response['last_fullnode_block_timestamp'])
#         ethereum_wallet_last_block.set(response['ethereum_wallet_last_block'])
#         ethereum_wallet_last_block_timestamp.set(response['ethereum_wallet_last_block_timestamp'])
#         ethereum_fullnode_status.set(response['ethereum_fullnode_status'])
#     else:
#         ethereum_fullnode_status.set(response['ethereum_fullnode_status'])


#     return generate_latest().decode()