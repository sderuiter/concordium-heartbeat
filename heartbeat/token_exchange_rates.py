# ruff: noqa: F403, F405, E402, E501, E722
from .utils import Utils
from sharingiscaring.mongodb import MongoDB, Collections, CollectionsUtilities
from sharingiscaring.tooter import TooterChannel, TooterType
from pymongo import ReplaceOne
from env import *
import aiohttp
import asyncio
import datetime as dt
from datetime import timezone
import dateutil


class ExchangeRates(Utils):
    async def coinapi(self, token):
        headers = {
            "X-CoinAPI-Key": COIN_API_KEY,
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            url = f"https://rest.coinapi.io/v1/exchangerate/{token}/USD"
            async with session.get(url) as resp:
                if resp.ok:
                    result = await resp.json()
                    return_dict = {
                        "_id": f"USD/{token}",
                        "token": token,
                        "timestamp": dateutil.parser.parse(result["time"]),
                        "rate": result["rate"],
                        "source": "CoinAPI",
                    }
                else:
                    return_dict = None

        return return_dict

    async def coingecko(self, token):
        # token_translation_dict = {
        #     "CCD": "concordium",
        #     "ETH": "ethereum",
        #     "USDC": "usd-coin",
        #     "USDT": "tether",
        #     "BTC": "bitcoin",
        #     "UNI": "unicorn-token",
        #     "LINK": "chainlink",
        #     "MANA": "decentraland",
        #     "AAVE": "aave",
        #     "DAI": "dai",
        #     "BUSD": "binance-usd",
        #     "VNXAU": "vnx-gold",
        #     "DOGE": "dogecoin",
        #     "SHIB": "shiba-inu",
        # }
        token_to_request = self.coingecko_token_translation.get(token)
        if token_to_request:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.coingecko.com/api/v3/simple/price?ids={token_to_request}&vs_currencies=usd&include_last_updated_at=true"
                async with session.get(url) as resp:
                    if resp.ok:
                        result = await resp.json()
                        result = result[token_to_request]
                        return_dict = {
                            "_id": f"USD/{token}",
                            "token": token,
                            "timestamp": dt.datetime.fromtimestamp(
                                result["last_updated_at"], tz=timezone.utc
                            ),
                            "rate": result["usd"],
                            "source": "CoinGecko",
                        }
                    else:
                        return_dict = None
        else:
            return_dict = None
        return return_dict

    async def get_token_translations_from_mongo(self):
        self.mongodb: MongoDB
        result = self.mongodb.utilities[
            CollectionsUtilities.token_api_translations
        ].find({"service": "coingecko"})
        self.coingecko_token_translation = {
            x["token"]: x["translation"] for x in list(result)
        }

    async def update_exchange_rates_for_tokens(self):
        await self.get_token_translations_from_mongo()
        if self.net == "testnet":
            pass
        else:
            while True:
                try:
                    token_list = [
                        x["_id"].replace("w", "")
                        for x in self.db[Collections.tokens_tags].find(
                            {"owner": "Arabella"}
                            # {"token_type": "fungible"}
                        )
                    ]
                    token_list.insert(0, "CCD")
                    token_list.insert(0, "EUROe")
                    queue = []
                    for token in token_list:
                        result = await self.coinapi(token)
                        if not result:
                            result = await self.coingecko(token)
                        if result:
                            queue.append(
                                ReplaceOne(
                                    {"_id": f"USD/{token}"},
                                    result,
                                    upsert=True,
                                )
                            )

                    if len(queue) > 0:
                        _ = self.mongodb.utilities[
                            CollectionsUtilities.exchange_rates
                        ].bulk_write(queue)

                        # update exchange rates retrieval
                        query = {"_id": "heartbeat_last_timestamp_exchange_rates"}
                        self.db[Collections.helpers].replace_one(
                            query,
                            {
                                "_id": "heartbeat_last_timestamp_exchange_rates",
                                "timestamp": dt.datetime.utcnow(),
                            },
                            upsert=True,
                        )

                except Exception as e:
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Failed to get exchange rates. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )

                await asyncio.sleep(60 * 5)

    async def coingecko_historical(self, token: str):
        """
        This is the implementation of the CoinGecko historical API.
        """
        # token_translation_dict = {
        #     "CCD": "concordium",
        #     "ETH": "ethereum",
        #     "USDC": "usd-coin",
        #     "USDT": "tether",
        #     "BTC": "bitcoin",
        #     "UNI": "unicorn-token",
        #     "LINK": "chainlink",
        #     "MANA": "decentraland",
        #     "AAVE": "aave",
        #     "DAI": "dai",
        #     "BUSD": "binance-usd",
        #     "VNXAU": "vnx-gold",
        #     "DOGE": "dogecoin",
        #     "SHIB": "shiba-inu",
        # }
        # token_to_request = token_translation_dict[token]
        token_to_request = self.coingecko_token_translation.get(token)
        return_list_for_token = []
        async with aiohttp.ClientSession() as session:
            url = f"https://api.coingecko.com/api/v3/coins/{token_to_request}/market_chart?vs_currency=usd&days=max&interval=daily&precision=full"
            async with session.get(url) as resp:
                if resp.ok:
                    result = await resp.json()
                    result = result["prices"]

                    for timestamp, price in result:
                        formatted_date = f"{dt.datetime.fromtimestamp(timestamp/1000, tz=timezone.utc):%Y-%m-%d}"
                        return_dict = {
                            "_id": f"USD/{token}-{formatted_date}",
                            "token": token,
                            "timestamp": timestamp,
                            "date": formatted_date,
                            "rate": price,
                            "source": "CoinGecko",
                        }
                        return_list_for_token.append(
                            ReplaceOne(
                                {"_id": f"USD/{token}-{formatted_date}"},
                                return_dict,
                                upsert=True,
                            )
                        )

                else:
                    return_dict = None

        return return_list_for_token

    async def update_exchange_rates_historical_for_tokens(self):
        if self.net == "testnet":
            pass
        else:
            while True:
                try:
                    token_list = [
                        x["_id"].replace("w", "")
                        for x in self.db[Collections.tokens_tags].find(
                            {"owner": "Arabella"}
                        )
                    ]
                    token_list.insert(0, "CCD")
                    queue = []
                    for token in token_list:
                        queue = await self.coingecko_historical(token)

                        if len(queue) > 0:
                            _ = self.mongodb.utilities[
                                CollectionsUtilities.exchange_rates_historical
                            ].bulk_write(queue)

                            # update exchange rates retrieval
                            query = {
                                "_id": "heartbeat_last_timestamp_exchange_rates_historical"
                            }
                            self.db[Collections.helpers].replace_one(
                                query,
                                {
                                    "_id": "heartbeat_last_timestamp_exchange_rates_historical",
                                    "timestamp": dt.datetime.utcnow(),
                                },
                                upsert=True,
                            )

                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message="Updated exchange rates historical.",
                        notifier_type=TooterType.INFO,
                    )
                except Exception as e:
                    self.tooter.send(
                        channel=TooterChannel.NOTIFIER,
                        message=f"Failed to get exchange rates historical. Error: {e}",
                        notifier_type=TooterType.REQUESTS_ERROR,
                    )

                eod_timeframe_start = dt.time(0, 1, 0)
                eod_timeframe_end = dt.time(1, 0, 0)

                if (dt.time() > eod_timeframe_start) and (
                    dt.time() < eod_timeframe_end
                ):
                    await asyncio.sleep(60 * 5)
                else:
                    await asyncio.sleep(60 * 60 * 4)
