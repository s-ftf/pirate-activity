#!/usr/bin/env python3
from pathlib import Path

# Base Paths
BASE_PATH = Path.home()
BIN_PATH = BASE_PATH / "pirate"
DATA_PATH = Path.home() / ".komodo" / "PIRATE"

# Variables used by the app
DAEMON = str(BIN_PATH / "pirated")
CLI = str(BIN_PATH / "pirate-cli")
TX = str(BIN_PATH / "pirate-tx")
DATADIR = str(DATA_PATH)
DEBUG_LOG = str(DATA_PATH / "debug.log")

# Known mining pool addresses
pool_addresses = {
    "coolmine_main":                'RTM2Aw6jiSrePbxZNpfFqz4bDpCcMECMiK',
    "coolmine_solo":                'RSiVR1jAnu95MJMdrZDLhsQacwAJ6aUmd9',
    "solopool.org":                 'RKE8ouuU2xJKmYNXNj9u9AAX4hxXY32fv3',
    "zergpool":                     'RAwQ7QzRymiFDrY1csXpAYEThLBGCpV235',
    "mining-dutch":                 'RXgVgBaQ1HwQmNiYu9EBoX9CFG6sDuxBPS',
    "piratepool.io-marketing":      'RD5PhyAUhapsvj5ps2cCHozsXZfQSvDdrZ', 
    "piratepool.io-explorer":       'RAzq6y7dsUKgfuzNjpzyGiuFzvrwuDheQw', 
    "piratepool.io-infastructure":  'RKnDd52zJJVtdLNrsLXnh926ojeuToFGiG', 
    "piratepool.io-miner-aoyouts":  'RRL95hu7Pfc4M5uzGL47CQ2rB2rLdpdreg',
    "zpool.ca":                     'RQoBHW1qMsAwTfZc77yYUmBeUxQKMbKKuT',
    "CoolMine.top":                 'RWoMaFmdMXS1Z4RDcTiMwjB53QhsdXVTpR',
    "CoolMine.top [SOLO]":          'RPpzLPu9RXeUqPy18rSKALetGXu7TnRLy4' 
}
