import json
from decimal import Decimal

from flask import current_app, g
from web3 import Web3, HTTPProvider
import decimal
import requests

from .. import events
from ..config import config
from ..models import Accounts, Settings, Wallets, db
from ..encryption import Encryption
from ..token import Token, Coin, get_all_accounts
from ..logging import logger
from . import api
from app import create_app
from ..unlock_acc import get_account_password

w3 = Web3(HTTPProvider(config["FULLNODE_URL"], 
                       request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))

w3l = Web3()

app = create_app()
app.app_context().push()

@api.post("/generate-address")
def generate_new_address(): 
    acc = w3l.eth.account.create()
    crypto_str = str(g.symbol)
    e = Encryption
    logger.warning(f'Saving wallet {acc.address} to DB')
    try:
        with app.app_context():
            db.session.add(Wallets(pub_address = acc.address, 
                                    priv_key = e.encrypt(acc.key.hex()),
                                    type = "regular",
                                    ))
            db.session.add(Accounts(address = acc.address, 
                                         crypto = crypto_str,
                                         amount = 0,
                                         ))
            db.session.commit()
            db.session.close()
            db.engine.dispose() 
    finally:
        with app.app_context():
            db.session.remove()
            db.engine.dispose() 

    logger.info(f'Added new address and wallet added to DB')
    return {'status': 'success', 'address': acc.address}

@api.post('/balance')
def get_balance():
    crypto_str = str(g.symbol)   
    if crypto_str == config["COIN_SYMBOL"]:
        inst = Coin(config["COIN_SYMBOL"])
        balance = inst.get_fee_deposit_coin_balance()
    else:
        if crypto_str in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
            token_instance = Token(crypto_str)
            balance = token_instance.get_fee_deposit_token_balance()
        else:
            return {'status': 'error', 'msg': 'token is not defined in config'}
    return {'status': 'success', 'balance': balance}

@api.post('/status')
def get_status():
    with app.app_context():
        pd = Settings.query.filter_by(name = 'last_block').first()
    
    last_checked_block_number = int(pd.value)
    block =  w3.eth.get_block(w3.to_hex(last_checked_block_number))
    return {'status': 'success', 'last_block_timestamp': block['timestamp']}

@api.post('/transaction/<txid>')
def get_transaction(txid):
    related_transactions = []
    
    list_accounts = get_all_accounts()
    if g.symbol == config["COIN_SYMBOL"]:
        try:
            transaction = w3.eth.get_transaction(txid)
            logger.warning(f"Checking transaction {txid}")
            if (transaction['to'] in list_accounts) or (transaction['from'] in list_accounts):
                logger.warning(f"Found related addresses in {txid}, checking it as a regular OP transaction")            
                if (transaction['to'] in list_accounts) and (transaction['from'] in list_accounts):
                    address = transaction["from"]
                    category = 'internal'
                elif transaction['to'] in list_accounts:
                    address = transaction["to"]
                    category = 'receive'
                elif transaction['from'] in list_accounts:                
                    address = transaction["from"]
                    category = 'send'
                amount = w3.from_wei(transaction["value"], "ether")
                confirmations = int(w3.eth.block_number) - int(transaction["blockNumber"])
                related_transactions.append([address, amount, confirmations, category])
            else:
                logger.warning(f"Addresses in {txid} is not related to any SHKeeper addresses. Checking {txid} as a smartcontract internal transaction")
                block_num = int(transaction["blockNumber"])
                block_eth_tx_addrs = []

                # check if there was regular eth transactions to our addresses in block
                block = w3.eth.get_block(block_num, True)       
                for tr in block.transactions:
                    if tr['to'] in list_accounts or tr['from'] in list_accounts:
                        block_eth_tx_addrs.append(tr['to'])
                        block_eth_tx_addrs.append(tr['from'])
                logger.warning(f"Regular ARB transactions to our addresses in {block_num} block: {block_eth_tx_addrs}")

                related_internal_addr = []

                for acc_addr in list_accounts:
                    if acc_addr[2:].lower() in transaction['input']:
                        if acc_addr not in block_eth_tx_addrs:
                            related_internal_addr.append(acc_addr)
                        else:
                            logger.warning(f"Found internal transaction to {acc_addr} but skip it because there was already a regular ARB transaction to {acc_addr} in {block_num} block")
                
                if len(related_internal_addr) > 0:
                    logger.warning(f"Found internal transactions to {related_internal_addr}")
                else:
                    logger.warning(f"Did not find any related addresses in tx {txid}")
                    return {'status': 'error', 'msg': 'txid is not related to any known address'}
                
                # check for other internal transactions to related addresses in this 
                token_addresses = []
                addresses_in_another_txs = []

                for token in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
                    token_addresses.append(config['TOKENS'][config["CURRENT_OP_NETWORK"]][token]['contract_address'])

                logger.warning("Checking block for another internal txs to related addresses")
                for tr in block.transactions:
                    if ((len(tr.input) > 6) and # check only txs with input (regular OP transactions input is '0x')
                        (tr['to'] not in token_addresses) and  # do not check internal tx to known token addresses and requested tx
                        tr['hash'].hex() != txid ):
                        for addr in related_internal_addr:
                            if addr[2:].lower() in tr.input:
                                logger.warning(f"Found another internal transaction {tr['hash'].hex()} to our address {addr} in the same block, skip it!")
                                addresses_in_another_txs.append(addr)
                
                clear_addresses = set(related_internal_addr) - set(addresses_in_another_txs)

                if len(clear_addresses) == 0:
                    logger.warning(f'No addresses are exclusively associated with the requested transaction in this block; return an empty list')

                for acc_addr in clear_addresses:
                    balance_before = Decimal(w3.from_wei(w3.eth.get_balance(acc_addr, block_num-1), "ether"))
                    balance_after = Decimal(w3.from_wei(w3.eth.get_balance(acc_addr, block_num), "ether"))
                    category = 'receive'
                    amount = balance_after - balance_before
                    confirmations = int(w3.eth.block_number) - int(transaction["blockNumber"])
                    if amount > 0: # skip 0 amount smartcontract tx 
                        related_transactions.append([acc_addr, amount, confirmations, category])

                if len(related_transactions) == 0:
                    logger.warning(f'There is not any transactions with amount > 0, respond with empty list')
                    
               
        except Exception as e:
             logger.warning({f'status': 'error', 'msg': str(e)})
             return []
    elif g.symbol in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
        token_instance  = Token(g.symbol)
        try:
            # transfer_abi_args = token_instance.contract._find_matching_event_abi('Transfer')['inputs']
            # for argument in transfer_abi_args:
            #     if argument['type'] == 'uint256':
            #         amount_name = argument['name']
            transactions_array = token_instance.get_token_transaction(txid)
            if len(transactions_array) == 0:
                logger.warning(f"There is not any token {g.symbol} transaction with transactionID {txid}")
                return {'status': 'error', 'msg': 'txid is not found for this crypto '}
            logger.warning(transactions_array)
            
            for transaction in transactions_array:
                if ((token_instance.provider.to_checksum_address(transaction['to']) in list_accounts) and 
                    (token_instance.provider.to_checksum_address(transaction['from']) in list_accounts)):
                    address = token_instance.provider.to_checksum_address(transaction["from"])
                    category = 'internal'
                    amount = Decimal(transaction["amount"]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                    confirmations = int(w3.eth.block_number) - int(transaction["block_number"])
                    related_transactions.append([address, amount, confirmations, category])

                elif token_instance.provider.to_checksum_address(transaction['to']) in list_accounts:
                    address = token_instance.provider.to_checksum_address(transaction["to"])
                    category = 'receive'
                    amount = Decimal(transaction["amount"]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                    confirmations = int(w3.eth.block_number) - int(transaction["block_number"])
                    related_transactions.append([address, amount, confirmations, category])
                elif token_instance.provider.to_checksum_address(transaction['from']) in list_accounts:                
                    address = token_instance.provider.to_checksum_address(transaction["from"])
                    category = 'send'
                    amount = Decimal(transaction["amount"]) / Decimal(10** (token_instance.contract.functions.decimals().call()))
                    confirmations = int(w3.eth.block_number) - int(transaction["block_number"])
                    related_transactions.append([address, amount, confirmations, category])
            if not related_transactions:
                logger.warning(f"txid {txid} is not related to any known address for {g.symbol}")
                return {'status': 'error', 'msg': 'txid is not related to any known address'}        
        except Exception as e:
            raise e 
    else:
        return {'status': 'error', 'msg': 'Currency is not defined in config'}
    logger.warning(related_transactions)
    return related_transactions


@api.post('/dump')
def dump():
    w = Coin(config["COIN_SYMBOL"])
    all_wallets = w.get_dump()
    return all_wallets


@api.post('/fee-deposit-account')
def get_fee_deposit_account():
    if g.symbol == config["COIN_SYMBOL"]:
        coin_instance = Coin(g.symbol)
        return {'account': coin_instance.get_fee_deposit_account(), 
                    'balance': coin_instance.get_fee_deposit_coin_balance()}
    elif g.symbol in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
        token_instance = Token(g.symbol)
        return {'account': token_instance.get_fee_deposit_account(), 
                'balance': token_instance.get_fee_deposit_account_balance()}
    else:
        raise Exception(f'Symbol {g.symbol} cannot be processed')

@api.post('/get_all_addresses')
def get_all_addresses():
    all_addresses_list = get_all_accounts()    
    return all_addresses_list


    
