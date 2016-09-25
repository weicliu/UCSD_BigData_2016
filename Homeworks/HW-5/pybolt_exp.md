# covtype

* Expected test error <= 13%
* Expected running time <= 350 seconds

### GBT

* depth=10; numIterations=10
    10 {'test': 0.14971938408404087, 'train': 0.14404584482195601}

* **depth=14; numIterations=15**
    14 {'test': 0.091270686429702111, 'train': 0.072091670001743241}
    253s

* depth=15; numIterations=15
    15 {'test': 0.078175277018276007, 'train': 0.055958083611801997}
    350s
    15 {'test': 0.080984314289825873, 'train': 0.05825376208914107}
    345s

* depth=20; numIterations=20
    time limit exceeded

### RANDOM

* depth=20; numTrees=20
    20 {'test': 0.16842135559073249, 'train': 0.16179991013707778}
    122s

* depth=29; numTrees=23
    29 {'test': 0.13506403799107786, 'train': 0.12245910132167243}
    210s

* depth=30; numTrees=23
    30 {'test': 0.13140883580371276, 'train': 0.11679970143903438}
    248s

* depth=30; numTrees=24
    30 {'test': 0.13265793639372572, 'train': 0.11911502208516353}
    236

* depth=30; numTrees=25
    30 {'test': 0.13222046337602533, 'train': 0.11811818201906764}
    276s


# higgs

* Expected test error <= 27.5%
* Expected running time <= 350 seconds

### GBT

* depth=9; numIterations=18
    9 {'test': 0.2754959528305897, 'train': 0.26410600086238695} 39 seconds

* depth=10; numIterations=10
    10 {'test': 0.28045396306844056, 'train': 0.26466187678256942}
    71s

* depth=10; numIterations=15
    10 {'test': 0.27518323081313795, 'train': 0.25628477471439925} 90 seconds

* depth=10; numIterations=17
    10 {'test': 0.27391108979068879, 'train': 0.25306770776511905} 55 seconds

* **depth=10; numIterations=18**
    10 {'test': 0.27371677708081588, 'train': 0.25229493633402428} 101 seconds
    10 {'test': 0.27425113703296639, 'train': 0.25099226449303597} 103 seconds
    10 {'test': 0.27453046155340866, 'train': 0.25247156980398883}
    10 {'test': 0.274229884080324, 'train': 0.25193777306755188} 103
    10 {'test': 0.27413879999757107, 'train': 0.25204297388422198}  104s

* depth=11; numIterations=18
    11 {'test': 0.27378660821092643, 'train': 0.23453418117398916}

    11 {'test': 0.27483407516258507, 'train': 0.23498615505301601}
    12 {'test': 0.27484318357086041, 'train': 0.21179262191605755}

* depth=10; numIterations=19
    10 {'test': 0.27461850950006983, 'train': 0.25242871021201213} 62 seconds

* depth=12; numIterations=15
    12 {'test': 0.27699276792382943, 'train': 0.21887484479632602} 165 seconds

* depth=15; numIterations=10
    15 {'test': 0.29261976038813964, 'train': 0.14306791556920137} 297 seconds


### RANDOM

* depth=10; numTrees=23
    10 {'test': 0.29175142546589511, 'train': 0.28636441562894505}

* depth=12; numTrees=20
    12 {'test': 0.28444951816520225, 'train': 0.27007517312677609}

* depth=15; numTrees=15
    15 {'test': 0.27964027859584778, 'train': 0.22915465299315804}
    80s

* depth=16; numTrees=18
    16 {'test': 0.27699276792382943, 'train': 0.20679103740992991}


* depth=17; numTrees=17
    17 {'test': 0.27705652678175646, 'train': 0.18437417203060955}
    145s

* depth=17; numTrees=18
    out of memory

* depth=18; numTrees=15


* depth=18; numTrees=17
    out of memory

* depth=18; numTrees=18
    out of memory

* depth=20; numTrees=20
    out of memory
