from collections import defaultdict
import requests as rq
import time
import ahocorasick

from web3 import Web3, HTTPProvider

from .models import Settings, db, Wallets, Accounts
from .config import config, get_contract_abi, get_contract_address
from .logging import logger
from .token import Token, get_all_accounts



w3 = Web3(HTTPProvider(config["FULLNODE_URL"], 
                       request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))

def handle_event(transaction):        
    logger.info(f'new transaction: {transaction!r}')


def walletnotify_shkeeper(symbol, txid) -> bool:
    """Notify SHKeeper about transaction"""
    logger.warning(f"Notifying about {symbol}/{txid}")
    while True:
        try:
            r = rq.post(
                    f'http://{config["SHKEEPER_HOST"]}/api/v1/walletnotify/{symbol}/{txid}',
                    headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']}).json()
            if r["status"] == "success":
                logger.warning(f"The notification about {symbol}/{txid} was successful")
                return True
            else:
                logger.warning(f"Failed to notify SHKeeper about {symbol}/{txid}, received response: {r}")
                time.sleep(5)
        except Exception as e:
            logger.warning(f'Shkeeper notification failed for {symbol}/{txid}: {e}')
            time.sleep(10)


def log_loop(last_checked_block, check_interval):
    from .tasks import drain_account
    from app import create_app
    app = create_app()
    app.app_context().push()

    token_addresses = []

    for token in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
        token_addresses.append(config['TOKENS'][config["CURRENT_OP_NETWORK"]][token]['contract_address'])

    while True:       
        
        last_block =  w3.eth.block_number
        if last_checked_block == '' or last_checked_block is None:
            last_checked_block = last_block

        if last_checked_block > last_block:
            logger.exception(f'Last checked block {last_checked_block} is bigger than last block {last_block} in blockchain')
        elif last_checked_block == last_block - 2:
            pass
        else:      
            list_accounts = set(get_all_accounts()) 
            for block_chunk in range((last_block - last_checked_block) // config['BLOCK_SCANNER_BATCH_SIZE']):
                start_batch_block = last_checked_block + 1
                last_batch_block = last_checked_block + config['BLOCK_SCANNER_BATCH_SIZE']
                logger.warning(f"Checking blocks {start_batch_block} - {last_batch_block}") 
                batch_time = time.time()
                batch = w3.batch_requests()
                for qq in range(config['BLOCK_SCANNER_BATCH_SIZE']):
                    batch.add(w3.eth.get_block(start_batch_block + qq, True))
                responses = batch.execute()
                assert len(responses) == config['BLOCK_SCANNER_BATCH_SIZE']
                batch_get_time = time.time()
                logger.warning(f"Get batch results: {batch_get_time - batch_time}")

#################### internal transaction detection part ####################
                # account_fragments = {addr[2:].lower() for addr in list_accounts} # for internal transaction check
    
                # A = ahocorasick.Automaton()
                # for idx, word in enumerate(account_fragments):
                #     A.add_word(word, (idx, word))
                # A.make_automaton()
#################### internal transaction detection part ####################

                for block in responses:  
                    block_txs = []    
                    block_internal_txs = []      
                    for transaction in block.transactions:
                        if transaction['to'] in list_accounts or transaction['from'] in list_accounts:
                            handle_event(transaction)
                            block_txs.append(transaction['to'].lower())
                            block_txs.append(transaction['from'].lower())
                            walletnotify_shkeeper(config["COIN_SYMBOL"], transaction['hash'].hex())
                            if ((transaction['to'] in list_accounts and transaction['from']  not in list_accounts) and 
                                ((w3.eth.block_number - last_batch_block) < 40)):
                                drain_account.delay(config["COIN_SYMBOL"], transaction['to'])
                
                    classic_trx_time = time.time()
                    logger.warning(f"Check classic transactions: {batch_get_time - classic_trx_time}")

                for token in config['TOKENS'][config["CURRENT_OP_NETWORK"]].keys():
                    token_instance  = Token(token)
                    transfers = token_instance.get_all_transfers(start_batch_block, last_batch_block)
                    for transaction in transfers:

                        if (token_instance.provider.to_checksum_address(transaction['from']) in list_accounts or 
                            token_instance.provider.to_checksum_address(transaction['to']) in list_accounts):
                            handle_event(transaction)
                            walletnotify_shkeeper(token, transaction['txid'])
                            if ((token_instance.provider.to_checksum_address(transaction['from']) not in list_accounts and 
                                token_instance.provider.to_checksum_address(transaction['to']) in list_accounts) and 
                                ((w3.eth.block_number - last_batch_block) < 40)):
                                drain_account.delay(token, token_instance.provider.to_checksum_address(transaction['to']))
                logger.warning(f"Check token transactions: {classic_trx_time - time.time()}")

#################### internal transaction detection part ####################
                    # start_t = time.time()

                    # for transaction in block.transactions:
                    #     if ((len(transaction.input) > 6) and # check only transactions with input (regular ARB transactions input is '0x')
                    #         (transaction['to'] not in token_addresses)): # do not check internal transactions to known token addresses
                    #         print(transaction)
                    #         for end_index, (idx, found_address) in A.iter(transaction.input.hex()):
                    #             logger.warning(f"Found internal transaction {transaction['hash'].hex()} to our address 0x{found_address}")
                    #             if (str('0x'+found_address) not in block_txs): # check if a regular ARB tx was in this block to this address  
                    #                 if (str('0x'+found_address) not in block_internal_txs):  # check if an internal tx to this address was in this block 
                    #                     block_internal_txs.append(str('0x'+found_address)) 
                    #                     walletnotify_shkeeper('ARB', transaction['hash'].hex())
                    #                     break # need only 1 notify to get all internal txs to our addresses
                    #                 else:
                    #                     logger.warning(f"There was already an internal transaction to 0x{found_address} in {block.blockNumber} block, skip notification")
                    #             else:
                    #                 logger.warning(f"There was already a regular ARB transaction to 0x{found_address} in {block.blockNumber} block, skip notification")


                    # finish_t = time.time()

                    # logger.warning(f"internal transaction check time {finish_t - start_t}")

#################### internal transaction detection part ####################

                last_checked_block = last_batch_block
                pd = Settings.query.filter_by(name = "last_block").first()
                pd.value = last_batch_block
                with app.app_context():
                    db.session.add(pd)
                    db.session.commit()
                    db.session.close()
        time.sleep(check_interval)

def events_listener():

    from app import create_app
    app = create_app()
    app.app_context().push()

    enough_accounts = False
    while not enough_accounts:
        list_accounts_ = set(get_all_accounts()) 
        if len(list_accounts_)==0:
            logger.warning("At least 1 account should be created to start scannnig")
            time.sleep(60)
        else:
            enough_accounts = True

    if (not Settings.query.filter_by(name = "last_block").first()) and (config['LAST_BLOCK_LOCKED'].lower() != 'true'):
        logger.warning(f"Changing last_block to a last block on a fullnode, because cannot get it in DB")
        with app.app_context():
            db.session.add(Settings(name = "last_block", 
                                         value = w3.eth.block_number))
            db.session.commit()
            db.session.close() 
            db.session.remove()
            db.engine.dispose()
    
    while True:
        try:
            pd = Settings.query.filter_by(name = "last_block").first()
            last_checked_block = int(pd.value)
            log_loop(last_checked_block, int(config["CHECK_NEW_BLOCK_EVERY_SECONDS"]))
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exception in main block scanner loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")           
            time.sleep(sleep_sec)


