from ccdefundamentals.mongodb import (
    MongoDB,
    MongoMotor,
    Collections,
    CollectionsUtilities,
)
from ccdefundamentals.GRPCClient import GRPCClient
from ccdefundamentals.tooter import Tooter
from pymongo import ASCENDING, DESCENDING, ReplaceOne
from env import *
from rich import print
import json

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

git_string = """
{
  "exchanges":
{
  "3kKn2kz9YHrkrKUcBZF9NUJbg8LqGzcBbLwzH1VPE5g2fTbY9z": "BitFinex",
  "4Gkp5YPScc9VieoxmemurZa8kEhiz2hcB9T9bASMXN8aJ5ZsPp": "BitGlobal",
  "4DAERy94ScrVZQPLyqV9bGF9kT8kDSUS8M5Y4Epmew49ZXjfnk": "MEXC",
  "3FEwBHfk7QBnTUDrqEowwn4txfKTnJyg66xLZza68rhYATh1HX": "ascendEX",
  "4f1fKZA4kT2s2knPHUJ2addLDpwH7kPe2SQQHrT8xetUDyhTc6": "KuCoin #1",
  "4sd3NCW8w4gC2dRZHSThT98URYgPJ6KMMnuawtLm4j1gbwpXRS": "KuCoin #2",
  "3K7JfzdRGJTNJ2bN8dXeyBvdYhTh9c2qpofBu8J7VFbi3wLYeA": "CoinEx"
},
"team/seed":
{
  "4qL7HmpqdKpYufCV7EvL1WnHVowy1CrTQS7jfYgef5stzpftwS": "Team/Seed-4q",
  "3nDiUUeTrHYrG2oqTCzz9tzg8SCpiR4LL7TFBjgKVEeBpkQEwE": "Team/Seed-3nD",
  "3bYDf2e6ijKCSctqkdLDTuwJ41zpZXrVyHu8ErJ75VBH4h24yR": "Team/Seed-3b",
  "3taQTr2vfAQmaKgStN6qWBwqAYpjCJatcugY7NK8SHQvUscCA8": "Team/Seed-3t",
  "47uKy77Ds7AEsTd7AAnyeesWL5wAyraVvafPEfhFw9dveuGym6": "Team/Seed-47",
  "3d4tsSiBrJXn2aiNHdfqMq2b9oGttoKn2LDCZbHHMm1WZi2c95": "Team/Seed-3d",
  "3nHTHHrit4wNgCRFkdvfFnLTL6AhViUE143eR7a6pMfWiazGKZ": "Team/Seed-3nH",
  "3oTmYTDG683bJvhMBCZd29SPcwpPsKmF8wc7XDL9zrsqpXC7mq": "Team/Seed-3o",
  "3C95KPAg8dzgyACEhU5B2b8pLpmNG67JoVgv4BNgnELqRjcU28": "Team/Seed-3C9",
  "3vE4c72bqeCegyUhNiebhvPXTGN1TQMCivAmdfZ1XqBrCutzzA": "Team/Seed-3v",
  "3ePAca51vQDPRvMPc74X77P2kBRBy78v3fTe7KEBfLFh257oMh": "Team/Seed-3e",
  "3WMhW7F8kCRpGAcRGxpd1Qxo6L6Keapjfx8kRv2mypWdXg9VHz": "Team/Seed-3W",
  "3aCihwgDAddwDpTVeqwjYMtbNtWHxRtqrMohQt81sDUivh3eUY": "Team/Seed-3a",
  "4H2aC1PRFWxb43sURKBCqGoqT3AxRGi9xtD5TAbXQGUXjhC5HH": "Team/Seed-4H",
  "3XwPBDyBVLS95TsWbZhAprZxd3NT12xWUXhnJRRssxiTDtgF2K": "Team/Seed-3X",
  "3HFSXTaofbefpTncBYKPwBHzKzVCVRBjs3W9ug5N56gahF1dcG": "Team/Seed-3H",
  "3C31UHhkhuKTgkSsRW2a3EUwsMYALuQp6ZofNFX9xUb7n5JJ4Y": "Team/Seed-3C3",
  "4bEqdbhvMbcktqhQsiuaRA46ZEe7o8PNTuGj5KhwYhStcgbGde": "Team/Seed-4b",
  "46KKpv3b68J4nfnQc1qA3X2QvcNc76cUqz5D4eULMk7SZKMgNB": "Team/Seed-46",
  "3mpvnZhWR6DJ4JLxbRFmhodvRZKdfTAXc8xeMNVGkRYXXH4ZJH": "Team/Seed-3m",
  "4BEUTv3eUv1HNLodttBVMWs7wxTwT1ATgLWQoSpjuDRC6snx3W": "Team/Seed-4B",
  "4icLUR2kyUBweu7Bjw2329smg9kQ69XbQhCMQ3NGYhar7vECo7": "Team/Seed-4i",
  "4YsuH4zBpBMsTNqabZQv8AxXmGCVTCYTU8nZgtReN6L5oVMPYa": "Team/Seed-4Y",
  "4VYm2qx7dViwhqobzznLBM4oeixZxDmWZ6pYCckczXePJ4ZvdZ": "Team/Seed-4V",
  "3USgJaoLTTtLF7o23qjPP2Tuis3X6PTwCTbkCNd8B3nTvtVoSW": "Team/Seed-3US",
  "4ZM4rmHztAkVMWVzCSvxckwxUSsAd5Sas1mbBenNpreePRYd6h": "Team/Seed-4ZM",
  "3Q6auaCXY27S9cw7wWxnL9NLRKeZ98BuhxNSwogkHEbApxt18C": "Team/Seed-3Q6",
  "43sPTsozscn3d2RbxHiaWy6Pp23KthNeNV6LKBhrco5SqoB3YW": "Team/Seed-43a",
  "4sNmzUZHf8i56SbSHHypzxwSVyS7CX2swTZpCLna6Mor6zGczf": "Team/Seed-4sN"
},
  "genesis":
  {
  "49SJ6R6T9zo1C5cLVyxbwAuZC3EcDB9a78vSQYm3ZLA2y2eojM": "Genesis-00",
  "47xTHwtFra1d4Mq4DYZuZYJEYrDXY34C4CGkTTzT6eiwjEczuT": "Genesis-01",
  "3CbvrNVpcHpL7tyT2mhXxQwNWHiPNYEJRgp3CMgEcMyXivms6B": "Genesis-02",
  "4d13WVDNKVGDUxRUb1PRQAJyTwWSVcjWS7uwZ1oqmDm5icQEPT": "Genesis-03",
  "3EctbG8WaQkTqZb1NTJPAFnqmuhvW62pQbywvqb9VeyqaFZdzN": "Genesis-04",
  "3ofwYFAkgV59BsHqzmiWyRmmKRB5ZzrPfbmx5nup24cE53jNX5": "Genesis-05",
  "4MPJybKC9Kz7kw9KNyLHhuAEt4ZTxLsd3DBDbxtKdUiv4fXqVN": "Genesis-06",
  "3XSLuJcXg6xEua6iBPnWacc3iWh93yEDMCqX8FbE3RDSbEnT9P": "Genesis-07",
  "44bxoGippBqpgseaiYPFnYgi5J5q58bQKfpQFeGbY9DHmDPD78": "Genesis-08",
  "3eUA4NnWufEqTBXR2QtTwjPxHZRGZvoqHaVjybmzZSqbuG32vJ": "Genesis-09",
  "32jTsKCtpGKr56WLweiPJB6jvLoCgFAHvfXtexHeoovJWu2PBD": "Genesis-10",
  "38T2PSXK6JVqNmoGqzjtnYsyb76uuNYKSpEoA8vPdtmRmpZhAv": "Genesis-11",
  "4CqVcmNi9F5V53YdJZ9U5sLaqaWt7Uxrf8VYk5WCLDYwbLL62Y": "Genesis-12",
  "4LH62AZmugKXFA2xXZhpoNbt2fFhAn8182kdHgxCu8cyiZGo2c": "Genesis-13"
  
  
  
},
  "first-day":
  {
    "3Bd2m1js7XQgt2cqsH27E43JXmCRDdzbWWtGjC3QtUVFRg83ya": "First-Day-14",
    "42Hc6eq8GaKpz7w9B7tGD1mNWLjbYFq3iSyK5NjNPNif7fgNVS": "First-Day-15",
    "3BjXtSWfjf2dgFg4gE2tBeTGrqRxoRKxW9HxaEn6UX7jJX1adF": "First-Day-16",
    "2xKDpRzaqUeSEeo3Bcaq7HRKmRaKRoz2KSY7DSsp9qGZdKfiLM": "First-Day-17",
    "4S9rLxtpQyYN9jfVtzzPN7Uof6XiYhhQwLjvDdN4xUeKmxbra3": "First-Day-18",
    "4Y9eZzPchSFargBz9toRNUbhkLEfxrm3tTzt4k6KG3j7vtvrn2": "First-Day-19",
    "3G5kPCKr39feVby843Ly4obLXXJLZJD3bKF8wSgKj1aho8oX7k": "First-Day-20",
    "4STEUWfV4WGbZP7HLybDTbVfMnp7pFH3CQsJPMrpCVZEV2wQ6P": "First-Day-21",
    "3ZQC7hNBKmrwAtTr1ipcT48iXydtRF6a7RCrHN2ZBJBM7KMKef": "First-Day-22",
    "4q7oubBouRtepGgEfNHrgpHtShLDJe8zybQVNhNsGpYuDV1b8U": "First-Day-23",
    "4FFUHFZ8dNXbywoJ2EnZjhikNZgt96reMLNdKdUhZTu7jAJYsS": "First-Day-24",
    "3sHsnFvcbjZdNs1epzXa4bH8ArDhSbr4oAZG3Wp9zJaQzXv1bg": "First-Day-25",
    "4KxTDMrzen9Fs8gnRzG4tuk6RE3tvc2HemDDchnaR9zXUirH5G": "First-Day-26",
    "4bhKc5rUDqBhSpBKhbeHnjNgk4xvxvtUXFFauFxVJsA2vykbBe": "First-Day-27",
    "4SbjBWUM6RYeZAcMjvEPY8f8XvXBzDYFo1S8VKfAtMTUYZ8nn8": "First-Day-28",
    "4KxTDMrzen9Fs8gnRzG4tuk6RE3tvc2HemDDchnaR9zXUirH5G": "First-Day-29",
    "3RszFQr5f5CeZKT64dDHEyPfm9R3QCedokexdw7rLEk96e4vXn": "First-Day-30",
    "4jhENRYSdCcSpmJbhnRyuZd83hrj3KDSKkDuBccmnwJP16g6CQ": "First-Day-31",
    "33ARkprdCcve8VpWbhdDDpjzwb5sXaxG84NUMisjWLBVyGrCoV": "First-Day-32",
    "4FvERgGjrq7pHkVnByBvSBkeogKQeTAVLsyQpZzLTqb15jGfLP": "First-Day-33",
    "4X3fJrEZFmeVkLJKHJuwzk8FqrFqSkF73zY3dvmrrpfDsf2CZG": "First-Day-34",
    "4Qgo54XRa84f8kpAhisqdzXBSp5UxpZ3xYV81aAoAHkkwtghmq": "First-Day-35",
    "38XGvXFqnEmoPSgv26bWPx7MCSgLdJZDcSC3zYPKwzFSsc1Ryi": "First-Day-36",
    "43hpfYSuZ9KGsEvvkW6w2E2HqwTUHGKuR8p8HWnAegUudiA9Tv": "First-Day-37",
    "3gef2dAVjhbZp6YnVKHPr4pza4Qp9nVNdv2vSXi5cc9Z5GVBTz": "First-Day-38",
    "3q12CQqD4SEEBm5NyJuvne5oechxU4ffx6UV3CEWokBYikBkUh": "First-Day-39",
    "4RnBuiDK4PYoLsX65NFJg88VTJYsKEUpNEqUoA1rKd4YUCu8Ln": "First-Day-40",
    "3mGDa6mnN96hgYn62m3BfjXB3Juk4UnmPDEpEGGQAsuTf9AVij": "First-Day-41",
    "49T2t9hbBgd1MwKgPU3HTNPqSrUvaK2ixjPQ2Svr4cUkyEPFx7": "First-Day-42",
    "4peF14JAh4cTvHnXSarsXNJN5Ziaz4Qys6pscFGNeV1q1vb2GD": "First-Day-43",
    "3Mys8V24r26RCuEJssBbnqquixdGMrKd47W4qSXcuRNkv4FMbc": "First-Day-44",
    "3Gs4viLqko2PpQCQo3BUQ7RFNBx1NVBoi8ZJBot31QNE7q81io": "First-Day-45",
    "3PHsJy5qsmrfL6S7Ypajb2vjMkZXfE43EH5sPbmgVKDvGcrV2o": "First-Day-46",
    "34eZC3vnz6hgWDXkh6LeR7dxo8nVWfWLr8r7WKnMtg5U1Ggh9u": "First-Day-47",
    "3qnQ37iwK6aTj5MdLxsYN8dDK4rWNT9476LHYo7YTNg1C65Dda": "First-Day-48",
    "3BfENMU1Bgsx5dNFvrGttmvwNSQjHZjS5Vhu7hUoF5iyZeQhLJ": "First-Day-49",
    "4PgPRbRijFj3jEWFcwKU69aysWB5hVb7u2ii9u7cSfNANQa1xM": "First-Day-50",
    "3TuJiMTTKjW9rzihytZ4oPUrB9QcEoAxZMZC9YcvHmh6hCKGVW": "First-Day-51",
    "4bJjjs9SyQK3uS7avU3A8oc1kXuffDhJpsT7ttY4mqSnGmcdKY": "First-Day-52",
    "3wdkVjFdRr6PJKzorxJxyM3cdwqcDVTKvXDhvf2DvE6qTkSxQ6": "First-Day-53",
    "4SmfpioUE6Fv2rqQR8cvxzmE5maGDJg3QxDECUwBbowLYYxfBA": "First-Day-55",
    "3Bk2xZdN6HCanxpZCMaFrPWeXMXakVcYtTYBzL6a32vngkUCL7": "First-Day-56",
    "3H61VEm7pzAK7XgAtEfDrU7KykoezrrwSrzQapJ76VcV72JPoX": "First-Day-57",
    "3aZbBHRqtue1CNC1UxRgDZ8bATc3a7JiSwSxu2cFqc5DWayVLp": "First-Day-58",
    "3VCSAN4YQMXs3nQfZFWcLvzW3e1eDJQM2XHbVMpsXLBmP3k8Me": "First-Day-59",
    "4oVq5rr8DYASSGA66FTdG2jQGSXS8LxevQweTY3WRmtKtAJxeF": "First-Day-60",
    "4rTNDoKuh2PxXvt9ARUjJFRmaPcaVJcZLzxCueR8TktyoKZ2pY": "First-Day-61",
    "4JoSnoctkSYeDXb7BANcHHNi7n268TYjFVXqe3r4opssfeY6Pc": "First-Day-62",
    "46iAKejVcN1C41njZjSBDmkVV3fwcnaiiQuunxWuDkPJ6UwN8d": "First-Day-63",
    "3XJSAF4fH6csesscLPYW9gQQbS7Fv9zVeeC1hwo8kkwRXwi8iX": "First-Day-64",
    "4TSZG5wPCYTJTUq6ZfQFNbJkPeSrDxPnnYkn2yG8FYUG6ZhPN9": "First-Day-65",
    "3pXHeDVKjJ5prbfbBqzZ3DRZG95V4iW2ELzU9iPvW6cCRb8gAn": "First-Day-66",
    "3mdZZmkawtAcogic7EXQjurbJit47aouY2gNxb12G4MNQthURa": "First-Day-67",
    "4LjMYFWBCKVRWPRC8cDfa5BiYhPvyf5ZMVzrqbdtKEFg8koX6T": "First-Day-68",
    "4JJGNCdRQ2tpFoZFeSrKKqzeJXy6XtLDtgvUam8MrBfy5djVUN": "First-Day-69",
    "3qMAw1SX6RJ2GCRpM3YCyYimKvmf7e4nLmsn7csHug2mfse4JB": "First-Day-70",
    "3me8FezaNRA9BDzhPJxAZDwQeAVJEK27JL7Z2fLeugVD2rmRFj": "First-Day-71",
    "37yP28tXhpAo6RixQskqM3k2uYYmsuV1r6rcYww4S3i3vwHsFQ": "First-Day-72",
    "3AXptodggm3sMz9qiByhpQNqDHb8dVn3BWtPHh27L5L4srR7FT": "First-Day-73",
    "352fpazuGSTXeeRFve7BkDkVzUfBPfT1orJggNnvHk72qRBYV3": "First-Day-74",
    "3ieGt8c6WFnGy2gXTmbvVogp1pdz6HcqkBGXepNpxCorzc8FGT": "First-Day-75",
    "4BSxinvHLqJewvX2CMmUVPyXjVCBeMHoK35c2NWKCx8qziKBNY": "First-Day-76",
    "4dd3mP61vKJjpWifQabR77xcKH1ocxLq7wFj5jVQnEGQnuWSR5": "First-Day-77",
    "3hDkrCo94T4gedjWYdTADCRw66WebPkhDdkjkwzG22S8dekQ55": "First-Day-78",
    "3eKUnNEpCS41hCRgEFdBeZicEBrVSPjxRJL57ekvbSL92UJTER": "First-Day-79",
    "3mwY1mKfoBHAESQQyEKNgtanZ9i6XhdTegvLZotsWF2setB12u": "First-Day-80",
    "34T1otYMGAYsYrq4JFxXANfS3QF19tShFNbWr75zTHv1t3TEpw": "First-Day-81",
    "38ugVUiqn7nQPEsLr5PYBpzoaB9Mrh8iWXA51J8cVCGoXDnjcJ": "First-Day-82",
    "3HEcykahopoaQ9ZrDh2T9e5ZmP6yFK6aJ9dTtZwfNCyDDZFw7G": "First-Day-83",
    "4MUCuM9pbtiu9wz6Bw7MMVRsNQGnMx7NTDTZUnfjni2BdwLxNj": "First-Day-84",
    "4Q7gEWsXYKJMp85BHK6w8yUxc4QGoepAJbyKCMVaQn2pixqGid": "First-Day-85",
    "3mdwwVWLQSuRiX6hkpDxz29XZ3tEE73KkSzbE1LVQjtiCTcAHU": "First-Day-86",
    "31cxb6bRfSTvKfd7Ssk2dSMRbybf4vGXaSg4P68yKjU6awoucb": "First-Day-87",
    "4RXPAomNg8quS6cRGZ4DDVXx2W9jCZNUuoAb3XczytvVLoi4k5": "First-Day-88",
    "3fDRmMQxTmEMG41jmbMkwxgWq6ULwy85iwVub6Tritr1KHe2nc": "First-Day-89",
    "3h6UhcgsEMU9ZUMtjKivBjGiQCNxNv6yFXwcj3L8JyvUD5W986": "First-Day-90",
    
    "4cVyfCBPscq166kWUGyoY74vTirSuDregvvzhiAxVQT5AJDWZ5": "First-Day-121"
  },
  "partner/private":
{
  
  "3KQuxkhL3q87aLwpLwFfoYSYB7zVc1gucqxwje4PqwJzSQ1sY1": "PP-Sale-3K",
  "3gGMHT3imcVeA6T9MZnmhXroL8nmW19GcNLcRQjzxK3HQDBgWe": "PP-Sale-3g",
  "3Xhdwp4Y2dXTTKaCs1G4QNy7vtUtmZDRzwtex1hSqw2ekUK7ZE": "PP-Sale-3Xh",
  "494XRkeP9mRHAe7o4bHWm5MEazuyJh9iZ2Nsn14bC1seNZ8yKR": "PP-Sale-49",
  "45inRtyRyzKmtENjvNfgnQ8NaGYaAeLpZ8373VwGJf4Y9mB9Hj": "PP-Sale-45",
  "4D44RYigFqPkABrRAHXSBBQqG4VNhXEsyJrt2GH6V2H8tS1tN3": "PP-Sale-4D",
  "3nxFD3fqtUG6YGAPbg2zBGa81Pt1SvNNTDjvvVakjvHVYw1biN": "PP-Sale-3nx",
  "3B85HEnBzjFcLm2ZN6Ggb9ig7CWT4vsBXM9j46v8o94stmzv9L": "PP-Sale-3b",
  "38DmwVuAwtCpscipaKGgAY1fs1huZS66LNqgyiMPTNk1NrgTzN": "PP-Sale-38",
  "3DLD6CpYYa41FmvYdjxTKBhgkM63NtHZGBjBPZgzgSArm1sueS": "PP-Sale-3DL",
  "4D1xtS8ztmxvuHy3ddiVQDnv96nwDRBj5bgWZx9uixMppgndzv": "PP-Sale-4D1",
  "4DwsKXJfvL9bwgfr4zCuMPB37iMYzHywiH7K5XCr37eWvwrAQg": "PP-Sale-4Dw",
  "3B5AmYmc3FgFc22PDcA5FyU6uC8JkMufzs9zTkrPxkRgkYAqDD": "PP-Sale-3B",
  "3n6kiufrNDYaFgqaGAyLZitRwQqyxnWQybruygc3Cx8WUpEGk6": "PP-Sale-3n6",
  "4prsbJTqccDvcRHaQs25J1wKfDXGUhYdphcshPRffMGbVarLYo": "PP-Sale-4p",
  "3rbknthWXXUiADWjh3G1fiPk7RfEuE9gj4Ugqsetsj3EUxvkoo": "PP-Sale-3r",
  "3oMhNeuM1tcPTRQY99EZxZojok4fyANGouA7xAbJ3WWiWiSy31": "PP-Sale-3o",
  "37S5Q3bDDaZr278vwK4Z1T9rbMoNSVFPjkos48W9Kz3kGJAiNg": "PP-Sale-37",
  "3nKBk9SKxCnL8A4w4WPKp6NxiVcd5YNmEcCBxY4YVGfWU8i9Hp": "PP-Sale-3nK",
  "36BzjKzW6w7N9SFDmnunbKypf8w2XzMupNte1d54SZ6RXeUKxD": "PP-Sale-36",
  "42MHeV4QcNVtidnUdDPdM2LedPb2QLKa7k1MQCXiRRPAZK3NtX": "PP-Sale-42",
  "4JG7grYgjAdnyA9nukGGU49ijmP1DKBQQGLppCMGW7BUSB2NnE": "PP-Sale-4J",
  "3Du7CKMaJoUqMAfg6cZWcW9Ng1LHvrMY2TJrwb4i4N75Cj9DBs": "PP-Sale-3Du",
  "31kpUd1TzsUnSu6xsq9P2dx95RedfE1hyFvS3o851obyMTTuLj": "PP-Sale-31",
  "33fUn8xxeQDpkAcAM6QTAv9XWMp3PjkT5nFFxXbCgJJDoPfsAD": "PP-Sale-33",
  "4eARZcdGRfUJLykk6GKHJaaqerUELqXUHCvpdcc4yHvSCBno3x": "PP-Sale-4e",
  "3NEK33i2SHV6XRqZg4QXAybEXPa3ku1oPaMZDXStdd2zSPfJKW": "PP-Sale-3N",
  "3JYq83DpKxhFxdgWgya9SceYCcm3Vrkvz5J9npxvy8M4z62v1H": "PP-Sale-3J",
  "2y1MWEc4vxYoJeK6aut3HorbazwmV8pcU5hoMXytUEK6e5Pgfa": "PP-Sale-2y",
  "3ANptDR19BcrGLKLq9mPma4XZfo1iNtrhEZrU6d7s4L8PFQ1q4": "PP-Sale-3A",
  "35AGYQC9ArExG1XsQtYrRgzNAMAeq9WN1mH7X2f7q5m7FmfZqx": "PP-Sale-35",
  "3K9xG3c73UWjpmYDQ2AEqgSTpUXuZyz5XK3NpwBxV4CecukGZa": "PP-Sale-3K",
  "3eXG5x4Je7Umn51SfaL5YVdWMR7RCwtUYyPZrN7yr6WJFELb2F": "PP-Sale-3e",
  "3aHv3xXYGz5cU66NJAwsoEH7VE98xn8t5EkvoCwKrG4LcRbLsZ": "PP-Sale-3a",
  "3wkjtUwE4UuHDHs8dFrz7HZ1RmPqsnJxAYWswErkHkEYVBWj5j": "PP-Sale-3w",
  "4k4gdQ6ekfBPzDN4a9fEfc8ZtUDe5yMV6kvEBoXs8toXgxWFjz": "PP-Sale-4k",
  "3VqpzqiHtHX1SqjdhLX38KHy29i1wWJ123pM6pBcGAa18ZZQvd": "PP-Sale-3V",
  "3wkjtUwE4UuHDHs8dFrz7HZ1RmPqsnJxAYWswErkHkEYVBWj5j": "PP-Sale-3Y",
  "3XCZnqSp4mjzGUiNVM6E7pDgw3bCocinCW5e9zwEbJuKa5mhW7": "PP-Sale-3XC",
  "346QiH9pVoMk5W67KMivM2oxewa3xLoo7bA2FxoRe93xg3DXe2": "PP-Sale-346",
  "4fpTce6HZYUqAjTBB83WSU21johsP4i3U6tp9kS3WtuRPG4a1U": "PP-Sale-4f",
  "437cES8dVEU62XX3G1SbFBRKvi8ewueHS7ADQgJjQSpiS5rHGu": "PP-Sale-43"
  
},
  "general":
  {
    "33BDZ6MhmQrX89HC36gCdVbAVWXVQfXBwJpydUgswCWy7JUyfD": "Airdropper",
    "38YTVPZsLKPEjsMoEvMkEN4tzE8ZmCnFzjCB5vTCKWJoaRKstp": "Seller #1",
    "3YzESUET7x4Xwq1TtPNT1gvQ3PTvwVAerNCjuiPkT4LmoCqTGH": "Seller #5",
    "4Qi2E8ht41LyBpxpvAKDSbef6UuBeZuoUv7fv7tLJjFVaaem7p": "Seller #6",
    "4kx448ZVKoELUMZJqzxBW7vEKJeJyzagoK3FcBxK1Kuck7zKBA": "MOTODEX MO",
    "4MwARWeXdMs3YZ5MPPn2561ceani6AJAVTNPtwS6tceaG2qatK": "AESIRX MO",
    "3suZfxcME62akyyss72hjNhkzXeZuyhoyQz1tvNSXY2yxvwo53": "Provenance MO",
    "4jU7HwXWMhSLXmHZd5gMucFR8EPXBMw9tbW7R4AKqNHEFEsAWQ": "Cryptogammon MO",
    "3b7Bfus88w9pVu5ajYReuTj8WhAgStrxNibvPDBN5c2EhSXfhd": "Climafi MO",
    "3CLkmo5gaKbxNXjPdDyU3mtGobB5dvLphGySxbEALQGMH2fzm4": "POAP MO",
    "3xthW4Pbm4emUvuneC2PLUFuk1EPBiFwKk6CoUty2BvmbTK897":" MySoMeID MO",
    "44Enrtf7pss6NG4st9ijtVAX5X1xgMh51B1DXJCaUoNjCQc4ZX": "Northstake",
    "4XMdmHd5bvcxqp1TLMeQhNjst5MJ6fydBhyMWqgFQtg9JUHUep": "Bridge Manager MO",
    "35CJPZohio6Ztii2zy1AYzJKvuxbGG44wrBn7hLHiYLoF2nxnh": "Data Registrar"
  }
}


 
 """

git_colors = """
{
   "exchanges": "#AE7CF7",
   "team/seed": "#70B785",
   "genesis": "#6E97F7",
   "partner/private": "#EE9B54",
   "general":  "#1FE8EB",
   "first-day": "#BFD8CB"
   
 }
"""

git_users = """
{
    "913126895": {
        "nodes": {
            "72723": {
                "ip": "31.21.31.76",
                "port": "10000"
            },
            "1246": {
                "ip": "92.109.79.250",
                "port": "10000"
            }
        },
        "bakers_to_follow": [
            72723
        ],
        "chat_id": 913126895,
        "first_name": "Sander",
        "language_code": "en",
        "token": "e2410784-fde1-11ec-a3b9-de5c44736176",
        "username": "sderuiter",
        "accounts_to_follow": [
            "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm",
            "3cunMsEt2M3o9Rwgs2pNdsCWZKB5MkhcVbQheFHrvjjcRLSoGP",
            "49HsifoFPMGtsYiAQW2RppQcnQsLm8hVeksaA8XBCPg8ttaTCa",
            "4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp"
        ],
        "labels": {
            "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm": "reza",
            "3cunMsEt2M3o9Rwgs2pNdsCWZKB5MkhcVbQheFHrvjjcRLSoGP": "explorer.ccd",
            "49HsifoFPMGtsYiAQW2RppQcnQsLm8hVeksaA8XBCPg8ttaTCa": "sprocker",
            "2zC7dHiCKoZ6CHeNcj3hXUadiCd5a7cBxTrV3VzpQ6uPkkPfZV": "Monique",
            "4hGN68SeYn9ZPSABU3uhS8nh8Tkv13DW2AmdZCneBzVkzeZ5Zp": "Web Wallet",
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio": "Robert",
            "4o2EjumtmyJE33cKZEaCNHgbfaykGBosdHdMWXg8s3zH4pw3hN": "CCDBaker",
            "4Ex1UHvyhKaab82aVaKbHNCmibb4pJxEdRP57Ckxzbki8ufSpm": "AMDAX",
            "3vyzKn8Gup52JFqNwvVgcHKXktqW5Bj82ATnfwATQZ7uJiCTpw": "Christoph"
        },
        "transactions_downloaded": {
            "3BFChzvx3783jGUKgHVCanFVxyDAn5xT3Y5NL5FKydVMuBa7Bm": 13,
            "3cunMsEt2M3o9Rwgs2pNdsCWZKB5MkhcVbQheFHrvjjcRLSoGP": 4,
            "4prsbJTqccDvcRHaQs25J1wKfDXGUhYdphcshPRffMGbVarLYo": 1,
            "36BzjKzW6w7N9SFDmnunbKypf8w2XzMupNte1d54SZ6RXeUKxD": 1,
            "4Ex1UHvyhKaab82aVaKbHNCmibb4pJxEdRP57Ckxzbki8ufSpm": 1,
            "3kKn2kz9YHrkrKUcBZF9NUJbg8LqGzcBbLwzH1VPE5g2fTbY9z": 1,
            "3K7JfzdRGJTNJ2bN8dXeyBvdYhTh9c2qpofBu8J7VFbi3wLYeA": 1
        },
        "transaction_limit_notifier": 500000,
        "smart_init": true,
        "smart_update": false,
        "cns_domain": true,
        "unstake_limit_notifier": 500000,
        "transaction_limit_notifier_to_exchange_only": false
    },
    "887562799": {
        "bakers_to_follow": [
            82548
        ],
        "chat_id": 887562799,
        "first_name": "MON | NEVER DM FIRST",
        "language_code": "es",
        "token": "c5d33180-feea-11ec-84eb-0242ac110005",
        "username": "monjsd",
        "accounts_to_follow": [
            "32a8TLfE67iJYgCFeCUFS7cECahEKP9ZkrHjF26nQi6c4qpzPr"
        ]
    },
    "1288180486": {
        "bakers_to_follow": [
            1317
        ],
        "chat_id": 1288180486,
        "first_name": "Monique",
        "language_code": "nl",
        "token": "9fed2e2e-feec-11ec-84eb-0242ac110005",
        "username": "MvdW75"
    },
    "505378727": {
        "bakers_to_follow": [
            711
        ],
        "chat_id": 505378727,
        "first_name": "Gattsu",
        "language_code": "tr",
        "token": "d71b46e2-feec-11ec-84eb-0242ac110005",
        "username": "BloodRocky"
    },
    "547592927": {
        "bakers_to_follow": [
            84424,
            85223
        ],
        "chat_id": 547592927,
        "first_name": "Jeppe Haugsrup",
        "language_code": "en",
        "token": "00f7cb96-feef-11ec-84eb-0242ac110005",
        "username": "haugstrup",
        "accounts_to_follow": [
            "3AoCqfjPvAUkQRonNTQ9qLeGnhdkrQ6hx7ULcnocX7gDjtX2YX",
            "4o2EjumtmyJE33cKZEaCNHgbfaykGBosdHdMWXg8s3zH4pw3hN"
        ],
        "labels": {
            "4o2EjumtmyJE33cKZEaCNHgbfaykGBosdHdMWXg8s3zH4pw3hN": "DAVID",
            "3AoCqfjPvAUkQRonNTQ9qLeGnhdkrQ6hx7ULcnocX7gDjtX2YX": "ORIENTAL"
        },
        "transaction_limit_notifier": 500000,
        "smart_init": true,
        "smart_update": true,
        "cns_domain": true,
        "unstake_limit_notifier": 100000
    },
    "912606863": {
        "bakers_to_follow": [
            830
        ],
        "chat_id": 912606863,
        "first_name": "\u00d6mer",
        "language_code": "en",
        "token": "63792d2c-fef0-11ec-84eb-0242ac110005",
        "username": "alkadeta",
        "accounts_to_follow": [
            "38kGkMJh4sBhPXPdZLjjxuSgzUXFtxNpnGavm9Xv67gABAtNP8"
        ]
    },
    "1146986275": {
        "bakers_to_follow": [
            704
        ],
        "chat_id": 1146986275,
        "first_name": "JP",
        "language_code": "en",
        "token": "bc1f0b92-fef3-11ec-84eb-0242ac110005",
        "username": "equinoxe10"
    },
    "1163519033": {
        "bakers_to_follow": [
            1916
        ],
        "chat_id": 1163519033,
        "first_name": "Mehmet",
        "language_code": "tr",
        "token": "66b28980-fef4-11ec-84eb-0242ac110005",
        "username": "ozkilicmemet",
        "accounts_to_follow": [
            "3nxFD3fqtUG6YGAPbg2zBGa81Pt1SvNNTDjvvVakjvHVYw1biN"
        ],
        "labels": {
            "3nxFD3fqtUG6YGAPbg2zBGa81Pt1SvNNTDjvvVakjvHVYw1biN": ""
        }
    },
    "1172545393": {
        "bakers_to_follow": [
            83458
        ],
        "chat_id": 1172545393,
        "first_name": "RS",
        "language_code": "es",
        "token": "c7c40032-fef4-11ec-84eb-0242ac110005",
        "username": "ramonsami",
        "accounts_to_follow": [
            "3DRdE2hYGje13pKrcYwXoD8sDmwVrfwLviJWBw2ynHoEBnGUsT",
            "3HN8wHFFJpbm5tADNiFEKXDy4dSFmsaWxeT7EzrfRP7p5GHhgz"
        ]
    },
    "5024239552": {
        "bakers_to_follow": [
            82826
        ],
        "chat_id": 5024239552,
        "first_name": "Svein",
        "language_code": "nb",
        "token": "cafa3a48-fefc-11ec-84eb-0242ac110005",
        "username": null
    },
    "345427192": {
        "bakers_to_follow": [],
        "chat_id": 345427192,
        "first_name": "amacar",
        "language_code": "sl",
        "token": "e136a288-ff06-11ec-84eb-0242ac110005",
        "username": "amacar",
        "accounts_to_follow": []
    },
    "2073909693": {
        "bakers_to_follow": [
            616
        ],
        "chat_id": 2073909693,
        "first_name": "Rush",
        "language_code": "tr",
        "token": "80bedbea-ff0c-11ec-84eb-0242ac110005",
        "username": "rushrush1"
    },
    "92681178": {
        "bakers_to_follow": [
            84292
        ],
        "chat_id": 92681178,
        "first_name": "Christian",
        "language_code": "de",
        "token": "036c6516-ff13-11ec-84eb-0242ac110005",
        "username": "c_matt"
    },
    "1095245972": {
        "bakers_to_follow": [
            67260
        ],
        "chat_id": 1095245972,
        "first_name": "Bobby",
        "language_code": "en",
        "token": "764cd586-ff1b-11ec-84eb-0242ac110005",
        "username": "Bobby_E32"
    },
    "1606223889": {
        "bakers_to_follow": [
            67260,
            67308
        ],
        "chat_id": 1606223889,
        "first_name": "Let's Go Brandon",
        "language_code": "zh-hans",
        "token": "f131179e-ff1b-11ec-84eb-0242ac110005",
        "username": "LittleCrab224"
    },
    "522288917": {
        "bakers_to_follow": [
            82821,
            84424
        ],
        "chat_id": 522288917,
        "first_name": "Flemming",
        "language_code": "en",
        "token": "20a437ca-ff3c-11ec-84eb-0242ac110005",
        "username": "flemmboo",
        "accounts_to_follow": [],
        "labels": {
            "3YAXEdg73XqU4D7wUuBTru2ygwpPQxYqMvX7pbLpBhTaMnFoZW": "",
            "3AoCqfjPvAUkQRonNTQ9qLeGnhdkrQ6hx7ULcnocX7gDjtX2YX": " "
        },
        "transaction_limit_notifier": 5000000
    },
    "5069327566": {
        "bakers_to_follow": [
            3442
        ],
        "chat_id": 5069327566,
        "first_name": "Tobias",
        "language_code": "da",
        "token": "7c35e4ee-ff50-11ec-84eb-0242ac110005",
        "username": "Grums",
        "accounts_to_follow": [
            "3WBCvk4CrHZxdc8gFV6Y8HvCpA4xF1caM8yDQa4buXPgH2uVzk",
            "Bitfinex"
        ]
    },
    "1529779670": {
        "bakers_to_follow": [
            780
        ],
        "chat_id": 1529779670,
        "first_name": "Richard",
        "language_code": "en",
        "token": "837339c8-ff8c-11ec-919f-0242ac110003",
        "username": "richardbeard",
        "accounts_to_follow": [
            "32YErNcHiNMusjMNRK1qNEgHJmtQVJToeyVaEBShxmr7iKc11w"
        ],
        "labels": {
            "32YErNcHiNMusjMNRK1qNEgHJmtQVJToeyVaEBShxmr7iKc11w": " "
        }
    },
    "285947021": {
        "bakers_to_follow": [
            476
        ],
        "chat_id": 285947021,
        "first_name": "PabloZsc",
        "language_code": "ru",
        "token": "dd02f974-ffa0-11ec-919f-0242ac110003",
        "username": "pablozsc",
        "accounts_to_follow": [],
        "labels": {}
    },
    "1414145307": {
        "bakers_to_follow": [],
        "chat_id": 1414145307,
        "first_name": "paul",
        "language_code": "fr",
        "token": "3d9a60f8-ffa9-11ec-919f-0242ac110003",
        "username": "Fuzzzzzzy"
    },
    "606792667": {
        "bakers_to_follow": [
            1637
        ],
        "chat_id": 606792667,
        "first_name": "Rasmus",
        "language_code": "da",
        "token": "d5ceb6a2-ffb4-11ec-919f-0242ac110003",
        "username": "rasmusory"
    },
    "11960425": {
        "bakers_to_follow": [
            452
        ],
        "chat_id": 11960425,
        "first_name": "Bj\u00f6rn",
        "language_code": "sv",
        "token": "faebea14-0021-11ed-919f-0242ac110003",
        "username": "beejay",
        "accounts_to_follow": [
            "3XJSAF4fH6csesscLPYW9gQQbS7Fv9zVeeC1hwo8kkwRXwi8iX",
            "4Ts2MHyF1kGadaCjcvBf6nNHp343oSixghxytYn4kdPCEnCNxN"
        ]
    },
    "504936281": {
        "bakers_to_follow": [],
        "chat_id": 504936281,
        "first_name": "Erhan",
        "language_code": "tr",
        "token": "b7271dae-006b-11ed-919f-0242ac110003",
        "username": "ErhanTEMURLENK"
    },
    "982948113": {
        "bakers_to_follow": [
            1043
        ],
        "chat_id": 982948113,
        "first_name": "Trekcir",
        "language_code": "en",
        "token": "8d26f1f0-00fc-11ed-8bfd-0242ac110003",
        "username": "trekcir",
        "labels": {
            "3nhGpEq1UNjpncHA6ZUkZzgNxXMfZRKTDJmsWXm4TH3aTBZb4j": "Account Rick"
        },
        "accounts_to_follow": [
            "3nhGpEq1UNjpncHA6ZUkZzgNxXMfZRKTDJmsWXm4TH3aTBZb4j"
        ],
        "unstake_limit_notifier": 1000000
    },
    "401998456": {
        "bakers_to_follow": [
            83458
        ],
        "chat_id": 401998456,
        "first_name": "Mounir",
        "language_code": "es",
        "token": "3b8ac090-0108-11ed-8bfd-0242ac110003",
        "username": "Ds_3D",
        "accounts_to_follow": [
            "3HN8wHFFJpbm5tADNiFEKXDy4dSFmsaWxeT7EzrfRP7p5GHhgz"
        ],
        "labels": {
            "3HN8wHFFJpbm5tADNiFEKXDy4dSFmsaWxeT7EzrfRP7p5GHhgz": "Baker"
        }
    },
    "1533816805": {
        "bakers_to_follow": [
            1317
        ],
        "chat_id": 1533816805,
        "first_name": "Tino",
        "language_code": "nl",
        "token": "937bc4d2-0137-11ed-9ba2-0242ac110003",
        "username": "tinonit",
        "labels": {}
    },
    "1783633068": {
        "bakers_to_follow": [
            82993
        ],
        "chat_id": 1783633068,
        "first_name": "GPC",
        "language_code": "en",
        "token": "ae3fba94-0227-11ed-9ba2-0242ac110003",
        "username": "gpc01",
        "labels": {
            "4raf9N4bGzZawy9qyo7hZJ6ywnKHt8h2Tz2urYZV3RRuZTJZBT": "Cambpool"
        },
        "accounts_to_follow": [
            "4raf9N4bGzZawy9qyo7hZJ6ywnKHt8h2Tz2urYZV3RRuZTJZBT"
        ]
    },
    "1149324847": {
        "bakers_to_follow": [
            1484
        ],
        "chat_id": 1149324847,
        "first_name": "far",
        "language_code": "es",
        "token": "76f2e30a-069f-11ed-9e85-0242ac110004",
        "username": "DisruptionRecall",
        "accounts_to_follow": [
            "37S5Q3bDDaZr278vwK4Z1T9rbMoNSVFPjkos48W9Kz3kGJAiNg"
        ],
        "labels": {
            "37S5Q3bDDaZr278vwK4Z1T9rbMoNSVFPjkos48W9Kz3kGJAiNg": "ffff"
        }
    },
    "23849762": {
        "bakers_to_follow": [
            1229
        ],
        "chat_id": 23849762,
        "first_name": "Moby",
        "language_code": "en",
        "token": "d606f6d2-081c-11ed-a325-0242ac110004",
        "username": null,
        "accounts_to_follow": [
            "3fdQDe32Spr73CZjTPN7MhRPErL5vw3PbAmHcKCcH8Ha4cHH8G"
        ],
        "labels": {
            "3fdQDe32Spr73CZjTPN7MhRPErL5vw3PbAmHcKCcH8Ha4cHH8G": " Moby"
        }
    },
    "1211453631": {
        "bakers_to_follow": [],
        "chat_id": 1211453631,
        "first_name": "Oxford",
        "language_code": "da",
        "token": "b2f124a0-0859-11ed-a325-0242ac110004",
        "username": "OxfordOxford",
        "accounts_to_follow": [],
        "labels": {}
    },
    "603806617": {
        "bakers_to_follow": [],
        "chat_id": 603806617,
        "first_name": "JJ",
        "language_code": "en",
        "token": "90daa8b0-0920-11ed-a325-0242ac110004",
        "username": "CriptoEvolution"
    },
    "913835442": {
        "bakers_to_follow": [
            82548
        ],
        "chat_id": 913835442,
        "first_name": "Sami",
        "language_code": "es",
        "token": "5d2478fc-0bff-11ed-815a-0242ac110004",
        "username": "Marisamipi",
        "accounts_to_follow": [
            "32a8TLfE67iJYgCFeCUFS7cECahEKP9ZkrHjF26nQi6c4qpzPr"
        ],
        "labels": {
            "32a8TLfE67iJYgCFeCUFS7cECahEKP9ZkrHjF26nQi6c4qpzPr": "CUMULO"
        }
    },
    "465888941": {
        "bakers_to_follow": [
            1164,
            1246
        ],
        "chat_id": 465888941,
        "first_name": "CryptoDenn",
        "language_code": "en",
        "token": "84871b70-0cb3-11ed-815a-0242ac110004",
        "username": "CryptoDenn",
        "accounts_to_follow": [
            "3d8buoAJQCGBbpQZPPSshm6otuSyCSUbWEgtRW9XdoHXCyyMNj",
            "3fVPb3FXkjhtnULFbUgs3es2Gazdjt8xKqG4PbSwgUa7erqQ23",
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio"
        ],
        "labels": {
            "3fVPb3FXkjhtnULFbUgs3es2Gazdjt8xKqG4PbSwgUa7erqQ23": " Desktop",
            "3d8buoAJQCGBbpQZPPSshm6otuSyCSUbWEgtRW9XdoHXCyyMNj": "Mobile",
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio": "Robert"
        }
    },
    "5245521284": {
        "bakers_to_follow": [],
        "chat_id": 5245521284,
        "first_name": "MikaelB",
        "language_code": "da",
        "token": "fea71d88-0db7-11ed-9681-0242ac110004",
        "username": "MikaelConcordium"
    },
    "915428160": {
        "bakers_to_follow": [
            37717
        ],
        "chat_id": 915428160,
        "first_name": "MBDenmark",
        "language_code": "da",
        "token": "32a291ee-0db8-11ed-9681-0242ac110004",
        "username": "MBDenmark",
        "accounts_to_follow": [],
        "labels": {
            "4GYp53AvAFfnFndzmveVNxU4agRZFQqFu6QFgUVkTgwRXZUpi5": "DrDoom"
        }
    },
    "1688388417": {
        "bakers_to_follow": [
            83458
        ],
        "chat_id": 1688388417,
        "first_name": "Josi\u00f1o",
        "language_code": "es",
        "token": "ded6de54-0e56-11ed-9681-0242ac110004",
        "username": "Josinhoicos",
        "accounts_to_follow": [
            "3HN8wHFFJpbm5tADNiFEKXDy4dSFmsaWxeT7EzrfRP7p5GHhgz"
        ],
        "labels": {
            "3HN8wHFFJpbm5tADNiFEKXDy4dSFmsaWxeT7EzrfRP7p5GHhgz": " "
        }
    },
    "1574600295": {
        "bakers_to_follow": [
            1246
        ],
        "chat_id": 1574600295,
        "first_name": "Robert",
        "language_code": "nl",
        "token": "139a5d3a-1101-11ed-9681-0242ac110004",
        "username": "Robert_3131",
        "accounts_to_follow": [
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio"
        ],
        "labels": {
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio": ""
        },
        "transactions_downloaded": {
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio": 1
        },
        "nodes": {}
    },
    "5256340869": {
        "bakers_to_follow": [
            654,
            1224,
            84252,
            84285,
            85192,
            85223
        ],
        "chat_id": 5256340869,
        "first_name": "David",
        "language_code": "da",
        "token": "009e8e60-1d8e-11ed-9816-0242ac110004",
        "username": null,
        "accounts_to_follow": [
            "2ziWWoVt3QJbaysPqge9qTMVk2xuTx8KWMzj9EzAP2Yzt3CQLX",
            "34QHRiPxx3WUTaRMHk4RoJEvtFav1Gykxfi6sv2fjrsyv9EDPr",
            "3FYJZJkxgMtby5tqkwhq8dKWztLkAJYdVKKR3EbgNkNPbuN9Bi",
            "3KQuxkhL3q87aLwpLwFfoYSYB7zVc1gucqxwje4PqwJzSQ1sY1",
            "3QNAtsjXZXuXcEqje5hzsEe4bd7LAsw73UWBMD94MSkkJ3eaqp",
            "3SJHNtBHRtyFDsaGmUYr3yxPFqQdfeNBA1UVhr19gjoRsLBbzi",
            "3Vn2oPmSAnMJCzSEgSqAqXyi4y5G7eWe6o9Lxx5pkWPjAxr9fV",
            "3YAXEdg73XqU4D7wUuBTru2ygwpPQxYqMvX7pbLpBhTaMnFoZW",
            "3YJ59emj7yPKngE2kwNh3S1oZz74agPhroimnHjF81nVhmX58U",
            "3cx1hJzqAC9LLMzM8gsNkAQGEngqehCSVthMwLuAdvCRWP1Kvz",
            "3e6UCZmQRMU1jhemPcBKrHBSm5jzikNH1Xi7zah3j2CRqKgoVD",
            "3nred1G4Vp5ve6A2fW5ficQPqTpNmxHSKHDhKejfQ2ig93sNak",
            "3pHmAtaTTM7ZdbhhGb1qumNzZpx4EoeGkXyRnqkfkb6WwZZ25j",
            "3qNt7dnN7V9rZBPCf4PKKpsGPeDe5afzPgsxhdN7abSLX7cW1v",
            "3wtTrocFGGFTZv8UHXX4MT9XyhwxMpuvZRgPaFJ24XG8xwWgpc",
            "3xMNnprhgwyqEEJ8G8w4UBdZuJyGN1NWcSQr1frVGPKGAMsZSA",
            "42MHeV4QcNVtidnUdDPdM2LedPb2QLKa7k1MQCXiRRPAZK3NtX",
            "46j4KBr3ZzDgg7aByeaAWkrUfjzTXPwTSAU8Edh9CHX58y74oz",
            "4D44RYigFqPkABrRAHXSBBQqG4VNhXEsyJrt2GH6V2H8tS1tN3",
            "4Ex1UHvyhKaab82aVaKbHNCmibb4pJxEdRP57Ckxzbki8ufSpm",
            "4FPi9A6iNJjEtKUmUbSfErVDM8L5jArTACNRu8hDFTxHsu3CJ9",
            "4KEmhchSWfmWPXANaRaPmDr4XcBWK5UFAVfnzGgKAcbv8ysZfS",
            "4aXhgKqw2kyxeNCHcvsiopPzrAdxcdP4zwBwGfBzNiwtHU7LBK",
            "4c5ZecpU9dkXpADJ7qR8LRqtnndaXRGD3cjB1v4MaAKzasvbHe",
            "4hzhPh4ANzDPQjSspDiZ9S6921Mp66S8nB9JuMQ1ct6MqCJ5Rx",
            "4jzp4tAGCstigJpJUFkacomqjuxcDHW9r6GNm37pHDRBPW95sJ",
            "4o2EjumtmyJE33cKZEaCNHgbfaykGBosdHdMWXg8s3zH4pw3hN",
            "4sfifnPAdMQ2XSe27j2JXLpWMhnDSH7gQUUGhb9HTvf63Gcb6z"
        ],
        "labels": {
            "4Ex1UHvyhKaab82aVaKbHNCmibb4pJxEdRP57Ckxzbki8ufSpm": "AMDAX accumulation account",
            "3YAXEdg73XqU4D7wUuBTru2ygwpPQxYqMvX7pbLpBhTaMnFoZW": "Vietnam",
            "3KQuxkhL3q87aLwpLwFfoYSYB7zVc1gucqxwje4PqwJzSQ1sY1": "2nd seller 050922",
            "3QNAtsjXZXuXcEqje5hzsEe4bd7LAsw73UWBMD94MSkkJ3eaqp": "David Mygind",
            "4D44RYigFqPkABrRAHXSBBQqG4VNhXEsyJrt2GH6V2H8tS1tN3": "AMDAX 1915 Baker & Pool",
            "4o2EjumtmyJE33cKZEaCNHgbfaykGBosdHdMWXg8s3zH4pw3hN": "DM Baker",
            "3wtTrocFGGFTZv8UHXX4MT9XyhwxMpuvZRgPaFJ24XG8xwWgpc": "Rogier CCDBAKER",
            "46j4KBr3ZzDgg7aByeaAWkrUfjzTXPwTSAU8Edh9CHX58y74oz": "Neukom CCDBAKER",
            "4hzhPh4ANzDPQjSspDiZ9S6921Mp66S8nB9JuMQ1ct6MqCJ5Rx": "Flemming Oriental invest CCDBAKER",
            "4c5ZecpU9dkXpADJ7qR8LRqtnndaXRGD3cjB1v4MaAKzasvbHe": "Willem Winterkonijn CCDBAKER",
            "3e6UCZmQRMU1jhemPcBKrHBSm5jzikNH1Xi7zah3j2CRqKgoVD": "Stockbull2 CCDBAKER",
            "3cx1hJzqAC9LLMzM8gsNkAQGEngqehCSVthMwLuAdvCRWP1Kvz": "Stockbull CCDBAKER",
            "3SJHNtBHRtyFDsaGmUYr3yxPFqQdfeNBA1UVhr19gjoRsLBbzi": "VIETNAM Y\u00fcz Bey CCDBAKER",
            "42MHeV4QcNVtidnUdDPdM2LedPb2QLKa7k1MQCXiRRPAZK3NtX": "42MH VIETNAM CCDBAKER",
            "34QHRiPxx3WUTaRMHk4RoJEvtFav1Gykxfi6sv2fjrsyv9EDPr": "VIETNAM CCDBAKER",
            "3xMNnprhgwyqEEJ8G8w4UBdZuJyGN1NWcSQr1frVGPKGAMsZSA": "Demedic CCDBAKER ",
            "3pHmAtaTTM7ZdbhhGb1qumNzZpx4EoeGkXyRnqkfkb6WwZZ25j": "Golybs CCDBAKER",
            "4jzp4tAGCstigJpJUFkacomqjuxcDHW9r6GNm37pHDRBPW95sJ": "VIETNAM MOR CCDBAKER",
            "3qNt7dnN7V9rZBPCf4PKKpsGPeDe5afzPgsxhdN7abSLX7cW1v": "Floris CCDBAKER",
            "4aXhgKqw2kyxeNCHcvsiopPzrAdxcdP4zwBwGfBzNiwtHU7LBK": "Eka Tuk Tuk CCDBAKER",
            "3Vn2oPmSAnMJCzSEgSqAqXyi4y5G7eWe6o9Lxx5pkWPjAxr9fV": "VIETNAM S\u00d8S CCDBAKER",
            "3nred1G4Vp5ve6A2fW5ficQPqTpNmxHSKHDhKejfQ2ig93sNak": "VIETNAM NEV\u00d8 CCDBAKER",
            "3FYJZJkxgMtby5tqkwhq8dKWztLkAJYdVKKR3EbgNkNPbuN9Bi": "VIETNAM JEPPE CCDBAKER",
            "2ziWWoVt3QJbaysPqge9qTMVk2xuTx8KWMzj9EzAP2Yzt3CQLX": "VIETNAM VEN CCDBAKER",
            "4sfifnPAdMQ2XSe27j2JXLpWMhnDSH7gQUUGhb9HTvf63Gcb6z": "VIETNAM JEPPE CCDBAKER",
            "3YJ59emj7yPKngE2kwNh3S1oZz74agPhroimnHjF81nVhmX58U": "VIETNAM JEPPE CCDBAKER",
            "4FPi9A6iNJjEtKUmUbSfErVDM8L5jArTACNRu8hDFTxHsu3CJ9": "North Stake",
            "4KEmhchSWfmWPXANaRaPmDr4XcBWK5UFAVfnzGgKAcbv8ysZfS": "Myrimidon"
        },
        "transaction_limit_notifier": 1000000,
        "smart_init": true,
        "smart_update": true,
        "cns_domain": true,
        "unstake_limit_notifier": 100000,
        "transactions_downloaded": {
            "4FPi9A6iNJjEtKUmUbSfErVDM8L5jArTACNRu8hDFTxHsu3CJ9": 1,
            "4KEmhchSWfmWPXANaRaPmDr4XcBWK5UFAVfnzGgKAcbv8ysZfS": 1,
            "3QNAtsjXZXuXcEqje5hzsEe4bd7LAsw73UWBMD94MSkkJ3eaqp": 4
        }
    },
    "790211026": {
        "bakers_to_follow": [],
        "chat_id": 790211026,
        "first_name": "Acidalia",
        "language_code": "en",
        "token": "70a15338-263e-11ed-b575-0242ac110004",
        "username": "Alien901",
        "accounts_to_follow": [
            "4S2qnFLa5YC3ANuGe6N9W5LB8b6AYzttF1aZwjhrvJnDezXDVZ"
        ],
        "labels": {
            "4S2qnFLa5YC3ANuGe6N9W5LB8b6AYzttF1aZwjhrvJnDezXDVZ": "Alien"
        }
    },
    "1062607902": {
        "bakers_to_follow": [],
        "chat_id": 1062607902,
        "first_name": "My\u2b55\ufe0f\u03feosmos",
        "language_code": "de",
        "token": "1207ab3e-3678-11ed-8111-0242ac110002",
        "username": "mygoldencosmos"
    },
    "5028075780": {
        "bakers_to_follow": [
            44954
        ],
        "chat_id": 5028075780,
        "first_name": "Mads",
        "language_code": "da",
        "token": "efb957ea-3969-11ed-9cd5-0242ac110002",
        "username": "Mads_Lerche",
        "accounts_to_follow": [
            "2xT9pFPc1RobNyz2JkzyP3oDBQSa7ptigK2gyZuS323khocyLJ",
            "3Ph3sDDMXSt6dPbTpZq7Bgkz7dvNAqVjut8nhVehTs6nqVK9Jt",
            "4cptmQbfj2hcudEdXzTmt9hCzCySPxEpMoF4nj7qfqJfWNFmdu"
        ],
        "labels": {
            "4cptmQbfj2hcudEdXzTmt9hCzCySPxEpMoF4nj7qfqJfWNFmdu": "Freedom Node",
            "2xT9pFPc1RobNyz2JkzyP3oDBQSa7ptigK2gyZuS323khocyLJ": "ArtSpace",
            "3Ph3sDDMXSt6dPbTpZq7Bgkz7dvNAqVjut8nhVehTs6nqVK9Jt": "Mads"
        }
    },
    "121303142": {
        "bakers_to_follow": [],
        "chat_id": 121303142,
        "first_name": "Anton",
        "language_code": "en",
        "token": "74f176fe-3b90-11ed-9cd5-0242ac110002",
        "username": "aromankov"
    },
    "1747200683": {
        "bakers_to_follow": [
            82826
        ],
        "chat_id": 1747200683,
        "first_name": "Jesper Mathias | northstake",
        "language_code": "en",
        "token": "3bbcb780-455a-11ed-83e8-0242ac110002",
        "username": "JesperMN",
        "accounts_to_follow": [
            "44Enrtf7pss6NG4st9ijtVAX5X1xgMh51B1DXJCaUoNjCQc4ZX"
        ],
        "labels": {
            "44Enrtf7pss6NG4st9ijtVAX5X1xgMh51B1DXJCaUoNjCQc4ZX": "Northstake Pool"
        }
    },
    "1453314087": {
        "bakers_to_follow": [
            1748
        ],
        "chat_id": 1453314087,
        "first_name": "Marnix",
        "language_code": "en",
        "token": "1c1d5bb4-4e10-11ed-ad8b-0242ac110002",
        "username": "marnixl",
        "accounts_to_follow": [
            "4r9wJzXxy31f7qDkwE43j5PZdLy6ENPWpoduMBTASLYZvScSxS"
        ],
        "labels": {
            "4r9wJzXxy31f7qDkwE43j5PZdLy6ENPWpoduMBTASLYZvScSxS": "Baker Marnix"
        }
    },
    "5027872502": {
        "bakers_to_follow": [],
        "chat_id": 5027872502,
        "first_name": "SAK.ccd",
        "language_code": "en",
        "token": "e3a7d57c-4fb6-11ed-913c-0242ac110002",
        "username": "Sak1511"
    },
    "1061435279": {
        "bakers_to_follow": [],
        "chat_id": 1061435279,
        "first_name": "Minrie | Bictory Finance",
        "language_code": "en",
        "token": "1f3249ec-4fb7-11ed-913c-0242ac110002",
        "username": "minriemacapugay"
    },
    "487892108": {
        "bakers_to_follow": [],
        "chat_id": 487892108,
        "first_name": "ralph.ccd",
        "language_code": "en",
        "token": "25d987ce-4fb7-11ed-913c-0242ac110002",
        "username": "ralphccd"
    },
    "5050957018": {
        "bakers_to_follow": [],
        "chat_id": 5050957018,
        "first_name": "Piet",
        "language_code": "nl",
        "token": "a2e41798-4fb7-11ed-913c-0242ac110002",
        "username": null
    },
    "782708051": {
        "bakers_to_follow": [],
        "chat_id": 782708051,
        "first_name": "Hoetoevallig",
        "language_code": "nl",
        "token": "7f363e56-4fb8-11ed-913c-0242ac110002",
        "username": "Hoetoevallig",
        "transaction_limit_notifier": 100000,
        "smart_init": true,
        "smart_update": true,
        "cns_domain": true
    },
    "429567058": {
        "bakers_to_follow": [],
        "chat_id": 429567058,
        "first_name": "Nelisss",
        "language_code": "en",
        "token": "adccdda0-4fb9-11ed-913c-0242ac110002",
        "username": "Jantjee"
    },
    "500634246": {
        "bakers_to_follow": [],
        "chat_id": 500634246,
        "first_name": "ced.ccd",
        "language_code": "en",
        "token": "7d0773e0-4fbb-11ed-913c-0242ac110002",
        "username": "ced000",
        "smart_init": false,
        "smart_update": false,
        "cns_domain": true
    },
    "1858150723": {
        "bakers_to_follow": [],
        "chat_id": 1858150723,
        "first_name": "Demediciakas",
        "language_code": "en",
        "token": "262596d0-4fc3-11ed-913c-0242ac110002",
        "username": null,
        "smart_init": true,
        "smart_update": false,
        "cns_domain": true
    },
    "511185503": {
        "bakers_to_follow": [],
        "chat_id": 511185503,
        "first_name": "Joha.ccd/eth |",
        "language_code": "en",
        "token": "a5746e9a-4fc6-11ed-913c-0242ac110002",
        "username": "johaslm",
        "smart_init": false,
        "smart_update": false,
        "cns_domain": true
    },
    "1669693848": {
        "bakers_to_follow": [],
        "chat_id": 1669693848,
        "first_name": "Martin",
        "language_code": "nl",
        "token": "18d629b6-5a97-11ed-92fb-0242ac110004",
        "username": "martin1981unknown",
        "accounts_to_follow": [
            "4jzNNQKrwci3C5VTMLuu8PPahcTfLr3xHZ4TaGmeMuMCg3HTj7"
        ],
        "labels": {
            "4jzNNQKrwci3C5VTMLuu8PPahcTfLr3xHZ4TaGmeMuMCg3HTj7": "Baker Marnix "
        }
    },
    "975069856": {
        "bakers_to_follow": [],
        "chat_id": 975069856,
        "first_name": "M.T.",
        "language_code": "da",
        "token": "90dd6728-61c0-11ed-8e7e-0242ac110004",
        "username": "matekdk",
        "transaction_limit_notifier": 100000,
        "unstake_limit_notifier": 500000,
        "accounts_to_follow": [],
        "labels": {
            "4sNmzUZHf8i56SbSHHypzxwSVyS7CX2swTZpCLna6Mor6zGczf": "Peter Klein",
            "4Ex1UHvyhKaab82aVaKbHNCmibb4pJxEdRP57Ckxzbki8ufSpm": "AMDAX accumulation account"
        }
    },
    "8166333": {
        "bakers_to_follow": [
            1246
        ],
        "chat_id": 8166333,
        "first_name": "Andreh6",
        "language_code": "nl",
        "token": "7833b514-650e-11ed-95f0-0242ac110005",
        "username": "Andreh6",
        "accounts_to_follow": [
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio"
        ],
        "labels": {
            "45XwJ7hZDJFZzJ94956iqwLHBa2EjPBUGLDsgJW73Ut2yCKDio": ""
        }
    },
    "596424151": {
        "bakers_to_follow": [
            85751
        ],
        "call_me": null,
        "chat_id": 596424151,
        "first_name": "J\u00e9r\u00f4me",
        "language_code": "fr",
        "tags": {},
        "token": "a0f81e42-6be7-11ed-8119-0242ac110005",
        "username": "JEROMEHTO",
        "accounts_to_follow": [
            "3CWk8pwjRsK1qG33cJ4QzvbScf5ytwXWtGT7pL5k8Vah17jZF8"
        ],
        "labels": {
            "3CWk8pwjRsK1qG33cJ4QzvbScf5ytwXWtGT7pL5k8Vah17jZF8": "HTOMINING"
        }
    }
}
"""

git_stuff = json.loads(git_string)
colors = json.loads(git_colors)
users = json.loads(git_users)
db_to_use = mongodb.utilities

# USERS
for chat_id, user in users.items():
    _ = db_to_use[CollectionsUtilities.users_prod].bulk_write(
        [
            ReplaceOne(
                {"_id": user["token"]},
                replacement=user,
                upsert=True,
            )
        ]
    )

    _ = db_to_use[CollectionsUtilities.users_dev].bulk_write(
        [
            ReplaceOne(
                {"_id": user["token"]},
                replacement=user,
                upsert=True,
            )
        ]
    )


# LABELS
# for label_group in git_stuff.keys():
#     for account, label in git_stuff[label_group].items():
#         d = {"_id": account, "label_group": label_group, "label": label}
#         _ = db_to_use[CollectionsUtilities.labeled_accounts].bulk_write(
#             [
#                 ReplaceOne(
#                     {"_id": account},
#                     replacement=d,
#                     upsert=True,
#                 )
#             ]
#         )

# COLORS
# for label_group, color in colors.items():
#     d = {"_id": label_group, "color": color}
#     _ = db_to_use[CollectionsUtilities.labeled_accounts_metadata].bulk_write(
#         [
#             ReplaceOne(
#                 {"_id": label_group},
#                 replacement=d,
#                 upsert=True,
#             )
#         ]
#     )


# all_heights_in_db = [
#     x["height"] for x in db_to_use[Collections.blocks].find({}, {"_id": 0, "height": 1})
# ]
# all_heights = set(range(0, max(all_heights_in_db)))
# missing_heights = list(set(all_heights) - set(all_heights_in_db))
# print(f"{len(missing_heights):,.0f} missing blocks on {net}.")
# print(missing_heights)


# result = [
#     x
#     for x in mongodb.mainnet[Collections.transactions].aggregate(pipeline)
#     if x["effect_count"] == 0
# ]
# print(
#     f"account_transaction.effects.{action_type}_configured: # txs with 0 effects: {len(result)}."
# )
# heights = [x["block_info"]["height"] for x in result]

# d = {"_id": "special_purpose_block_request", "heights": heights}
# _ = mongodb.mainnet[Collections.helpers].bulk_write(
#     [
#         ReplaceOne(
#             {"_id": "special_purpose_block_request"},
#             replacement=d,
#             upsert=True,
#         )
#     ]
# )
pass
