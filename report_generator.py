from sharingiscaring.mongodb import MongoDB, Collections, MongoTypeInvolvedAccount
from sharingiscaring.GRPCClient import GRPCClient
from sharingiscaring.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from env import *
import datetime as dt
from datetime import timedelta
from sharingiscaring.GRPCClient.CCD_Types import CCD_BlockItemSummary
from enum import Enum
import calendar


class ExchangeAccountInfo(Enum):
    ascendex = ["3FEwBHfk7QBnTUDrqEowwn4txfKTnJyg66xLZza68rhYATh1HX"]
    bitglobal = ["4Gkp5YPScc9VieoxmemurZa8kEhiz2hcB9T9bASMXN8aJ5ZsPp"]
    mexc = ["4DAERy94ScrVZQPLyqV9bGF9kT8kDSUS8M5Y4Epmew49ZXjfnk"]
    kucoin = [
        "4f1fKZA4kT2s2knPHUJ2addLDpwH7kPe2SQQHrT8xetUDyhTc6",
        "4sd3NCW8w4gC2dRZHSThT98URYgPJ6KMMnuawtLm4j1gbwpXRS",
    ]

    bitfinex = ["3kKn2kz9YHrkrKUcBZF9NUJbg8LqGzcBbLwzH1VPE5g2fTbY9z"]
    coinex = ["3K7JfzdRGJTNJ2bN8dXeyBvdYhTh9c2qpofBu8J7VFbi3wLYeA"]


all_exch_accounts_canonical = [[y[:29] for y in x.value] for x in ExchangeAccountInfo]
all_exch_accounts_canonical = [
    item for sublist in all_exch_accounts_canonical for item in sublist
]


class BuyerSeller(Enum):
    Buyer = "Buyers"
    Seller = "Seller"


class ExchangeStatistic(Enum):
    # if this is postive, it's more buyers than seller)
    net_flow = "Net Flow"


class SideStatistic(Enum):
    count_of_unique_accounts = "Unique Accounts"
    sum_of_all_amounts = "Sum of Amounts"
    average_of_amount_per_account = "Average Tx Amount"


class SideInfo:
    def __init__(self, side: BuyerSeller):
        self.side = side
        self.trades = []
        self.unique_accounts = []
        self.statistics: dict[SideStatistic, float] = {}

    def start_calculations_from_trader(self, trades: list[MongoTypeInvolvedAccount]):
        self.trades = trades
        self.amounts = [x.amount / 1_000_000 for x in self.trades]
        if self.side == BuyerSeller.Buyer:
            accounts = [x.receiver_canonical for x in self.trades]
        else:
            accounts = [x.sender_canonical for x in self.trades]
        self.unique_accounts = set(accounts)
        self.statistics[SideStatistic.count_of_unique_accounts] = len(
            self.unique_accounts
        )

        self.statistics[SideStatistic.sum_of_all_amounts] = sum(self.amounts)

        if self.statistics[SideStatistic.count_of_unique_accounts] > 0:
            self.statistics[SideStatistic.average_of_amount_per_account] = (
                sum(self.amounts)
                / self.statistics[SideStatistic.count_of_unique_accounts]
            )
        else:
            self.statistics[SideStatistic.average_of_amount_per_account] = 0


class ExchangeAccountReport:
    def __init__(self, exchange: ExchangeAccountInfo, block_start: int, block_end: int):
        self.exchange = exchange
        self.exchange_name = self.exchange.name
        self.exchange_addresses = self.exchange.value
        self.exchange_address_canonicals = [x[:29] for x in self.exchange_addresses]
        self.block_start = block_start
        self.block_end = block_end
        self.side: dict[BuyerSeller:SideInfo] = {
            BuyerSeller.Buyer: SideInfo(side=BuyerSeller.Buyer),
            BuyerSeller.Seller: SideInfo(side=BuyerSeller.Seller),
        }
        self.exchange_statistics: dict[ExchangeStatistic, float] = {}

    def generate_side_info(self, side: BuyerSeller):
        """
        A buyer is an account that has an account_transfer where the exchange account
        is the sender of the transaction and the account is the receiver.
        A seller is an account that has an account_transfer where the exchange account
        is the receiver of the transaction and the account is the sender.
        """
        self.side[side] = SideInfo(side=side)
        if side == BuyerSeller.Buyer:
            match_on_address = "sender_canonical"
        else:
            match_on_address = "receiver_canonical"
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"sender_canonical": {"$nin": all_exch_accounts_canonical}},
                        {"receiver_canonical": {"$nin": all_exch_accounts_canonical}},
                    ]
                }
            },
            {
                "$match": {
                    f"{match_on_address}": {"$in": self.exchange_address_canonicals}
                }
            },
            {"$match": {"block_height": {"$gte": self.block_start}}},
            {"$match": {"block_height": {"$lte": self.block_end}}},
        ]
        trades = [
            MongoTypeInvolvedAccount(**x)
            for x in mongodb.mainnet[Collections.involved_accounts_transfer].aggregate(
                pipeline
            )
        ]
        self.side[side].start_calculations_from_trader(trades)
        pass

    def calculate_exchange_statistics(self):
        self.exchange_statistics[ExchangeStatistic.net_flow] = (
            self.side[BuyerSeller.Buyer].statistics[SideStatistic.sum_of_all_amounts]
            - self.side[BuyerSeller.Seller].statistics[SideStatistic.sum_of_all_amounts]
        )


def get_period(mongodb: MongoDB, start_day: str, end_day: str):
    start_record = mongodb.mainnet[Collections.blocks_per_day].find_one(
        {"_id": start_day}
    )
    end_record = mongodb.mainnet[Collections.blocks_per_day].find_one({"_id": end_day})
    if not end_record:
        end_record = {"height_for_last_block": 1_000_000_000}

    return start_record["height_for_first_block"], end_record["height_for_last_block"]


tooter: Tooter = Tooter(
    ENVIRONMENT, BRANCH, NOTIFIER_API_TOKEN, API_TOKEN, FASTMAIL_TOKEN
)
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD", MONGODB_PASSWORD_LOCAL)
grpcclient = GRPCClient()
mongodb = MongoDB(
    {
        "MONGODB_PASSWORD": MONGODB_PASSWORD,
    },
    tooter,
)
periods = {}
exchanges = {}
period_strings = [
    "2022-03",
    "2022-04",
    "2022-05",
    "2022-06",
    "2022-07",
    "2022-08",
    "2022-09",
    "2022-10",
    "2022-11",
    "2022-12",
    "2023-01",
    "2023-02",
    "2023-03",
    "2023-04",
]

for exchange in ExchangeAccountInfo:
    exchanges[exchange] = {}
    exch: dict[str:ExchangeAccountReport] = {}
    print("\n", exchange.name)

    for report_date in period_strings:
        periods[report_date] = {}
        print(report_date, end=" | ")
        year = int(report_date[:4])
        month = int(report_date[-2:])
        res = calendar.monthrange(year, month)

        block_start, block_end = get_period(
            mongodb,
            f"{year}-{month:02}-01",
            f"{year}-{month:02}-{res[1]:02}",
        )

        exch_for_period = ExchangeAccountReport(exchange, block_start, block_end)

        exch_for_period.generate_side_info(BuyerSeller.Buyer)
        exch_for_period.generate_side_info(BuyerSeller.Seller)
        exch_for_period.calculate_exchange_statistics()

        exchanges[exchange][report_date] = exch_for_period

# reverse to per period
for exchange, e in exchanges.items():
    for report_date, exch_for_period in e.items():
        periods[report_date][exchange] = exch_for_period

html = '<html><head><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3" crossorigin="anonymous"></head><body>'
html += "<h1>Exchanges</h1>"
for exchange, e in exchanges.items():
    html += f"<h5>{exchange.name}</h5>"
    html += '<table class="table table-sm table-striped">'
    html += '<thead class="thead-dark"><tr>'
    html += "<th>Month</th>"
    html += '<th class="text-end">B Un. Accounts</th>'
    html += '<th class="text-end">B Avg Amounts</th>'
    html += '<th class="text-end">S Un. Accounts</th>'
    html += '<th class="text-end">S Avg Amounts</th>'
    html += '<th class="text-end">Net Flow (+ = B on exch)</th></tr></thead>'

    sum_net_flow = 0
    sum_amount = {BuyerSeller.Buyer: 0, BuyerSeller.Seller: 0}
    unique_accounts = {BuyerSeller.Buyer: [], BuyerSeller.Seller: []}
    avg_per_unique_account = {BuyerSeller.Buyer: 0, BuyerSeller.Seller: 0}
    for report_date, exch in e.items():
        if (len(exch.side[BuyerSeller.Buyer].trades) == 0) and (
            len(exch.side[BuyerSeller.Seller].trades) == 0
        ):
            pass
        else:
            sum_net_flow += exch.exchange_statistics[ExchangeStatistic.net_flow]

            for bs in BuyerSeller:
                sum_amount[bs] += exch.side[bs].statistics[
                    SideStatistic.sum_of_all_amounts
                ]
                unique_accounts[bs].extend(list(exch.side[bs].unique_accounts))

            html += f"<tr><td>{report_date}</td>"

            html += f'<td class="text-end">{len(exch.side[BuyerSeller.Buyer].unique_accounts):,.0f}</td>'
            html += f'<td class="text-end">{exch.side[BuyerSeller.Buyer].statistics[SideStatistic.average_of_amount_per_account]:,.0f} CCD</td>'
            html += f'<td class="text-end">{len(exch.side[BuyerSeller.Seller].unique_accounts):,.0f}</td>'
            html += f'<td class="text-end">{exch.side[BuyerSeller.Seller].statistics[SideStatistic.average_of_amount_per_account]:,.0f} CCD</td>'
            html += f'<td class="text-end font-monospace">{(exch.exchange_statistics[ExchangeStatistic.net_flow]):,.0f} CCD</td>'
            html += "</tr>"

    for bs in BuyerSeller:
        unique_accounts[bs] = set(unique_accounts[bs])
        avg_per_unique_account[bs] = sum_amount[bs] / len(unique_accounts[bs])
    html += f'<tr><td><i>Totals</i></td><td class="text-end"><i>{len(unique_accounts[BuyerSeller.Buyer]):,.0f}</i></td><td class="text-end"><i>{avg_per_unique_account[BuyerSeller.Buyer]:,.0f} CCD</i></td><td class="text-end"><i>{len(unique_accounts[BuyerSeller.Seller]):,.0f}</i></td><td class="text-end"><i>{avg_per_unique_account[BuyerSeller.Seller]:,.0f} CCD</i></td><td class="text-end border-top font-monospace"><i>{(sum_net_flow):,.0f} CCD</i></td>'
    html += "</table>"
html += "</body></html><br/>"


html += "<h1>Periods</h1>"
for period, e in periods.items():
    html += f"<br/><h5>{period}</h5>"
    html += '<table class="table table-sm table-striped">'
    html += '<thead class="thead-dark"><tr>'
    html += "<th>Exchange</th>"
    html += '<th class="text-end">B Un. Accounts</th>'
    html += '<th class="text-end">B Avg Amounts</th>'
    html += '<th class="text-end">S Un. Accounts</th>'
    html += '<th class="text-end">S Avg Amounts</th>'
    html += '<th class="text-end">Net Flow (+ = B on exch)</th></tr></thead>'

    sum_net_flow = 0
    sum_amount = {BuyerSeller.Buyer: 0, BuyerSeller.Seller: 0}
    unique_accounts = {BuyerSeller.Buyer: [], BuyerSeller.Seller: []}
    avg_per_unique_account = {BuyerSeller.Buyer: 0, BuyerSeller.Seller: 0}
    for exchange, exch in e.items():
        if (len(exch.side[BuyerSeller.Buyer].trades) == 0) and (
            len(exch.side[BuyerSeller.Seller].trades) == 0
        ):
            pass
        else:
            sum_net_flow += exch.exchange_statistics[ExchangeStatistic.net_flow]

            for bs in BuyerSeller:
                sum_amount[bs] += exch.side[bs].statistics[
                    SideStatistic.sum_of_all_amounts
                ]
                unique_accounts[bs].extend(list(exch.side[bs].unique_accounts))

            html += f"<tr><td>{exchange.name}</td>"

            html += f'<td class="text-end">{len(exch.side[BuyerSeller.Buyer].unique_accounts):,.0f}</td>'
            html += f'<td class="text-end">{exch.side[BuyerSeller.Buyer].statistics[SideStatistic.average_of_amount_per_account]:,.0f} CCD</td>'
            html += f'<td class="text-end">{len(exch.side[BuyerSeller.Seller].unique_accounts):,.0f}</td>'
            html += f'<td class="text-end">{exch.side[BuyerSeller.Seller].statistics[SideStatistic.average_of_amount_per_account]:,.0f} CCD</td>'
            html += f'<td class="text-end font-monospace">{(exch.exchange_statistics[ExchangeStatistic.net_flow]):,.0f} CCD</td>'
            html += "</tr>"
    for bs in BuyerSeller:
        unique_accounts[bs] = set(unique_accounts[bs])
        avg_per_unique_account[bs] = sum_amount[bs] / len(unique_accounts[bs])
    html += f'<tr><td><i>Totals</i></td><td class="text-end"><i>{len(unique_accounts[BuyerSeller.Buyer]):,.0f}</i></td><td class="text-end"><i>{avg_per_unique_account[BuyerSeller.Buyer]:,.0f} CCD</i></td><td class="text-end"><i>{len(unique_accounts[BuyerSeller.Seller]):,.0f}</i></td><td class="text-end"><i>{avg_per_unique_account[BuyerSeller.Seller]:,.0f} CCD</i></td><td class="text-end border-top font-monospace"><i>{(sum_net_flow):,.0f} CCD</i></td>'
    html += "</table>"
html += "</body></html>"

textfile = open("report.html", "w")
textfile.write(html)
textfile.close()
