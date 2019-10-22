import os

from stopwatch import Stopwatch

import config
from config import logger_factory

import statistics

from services.AutoMlPredictionService import AutoMlPredictionService
from utils import date_utils

logger = logger_factory.create_logger(__name__)

def predict_and_calculate():
  # short_model_id = "ICN7780367212284440071"  # fut_prof 68%
  # short_model_id = "ICN5723877521014662275" # closeunadj
  # short_model_id = "ICN2013469796611448097" # multi3 58.7%
  # short_model_id = "ICN7558148891127523672"  # multi3 57.5%
  # short_model_id = "ICN2174544806954869914"  # multi3 57.5%
  # short_model_id = "ICN2383257928398147071" # vol_eq 65%
  # short_model_id = "ICN200769567050768296"  # voleq_rec Sept %
  # short_model_id = "ICN1127515596385130617" # voleq 2018-06-18 66%
  # short_model_id = "ICN5283794452616644197" # volreq_fil_09_22_v20190923042914 7/17-8/30: 0.2637%
  # short_model_id = "ICN8748316622033513158" # fiddy with 19 vol acc:62
  # short_model_id = "ICN7150281105604556161" # deuce_eq min_price: mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
  # short_model_id = "ICN6329887226779646397" # deuce 1.1%; mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
  # short_model_id = "ICN323980058926307012" # tiny
  # short_model_id = "ICN1872651771008733456" # tin_big; limited set
  # short_model_id = "ICN7012160376262305340" # tin_big II;
  # short_model_id = "ICN2140008179580940274" # millville_1:
  # short_model_id = "ICN1615151565178573620"  # bigname I:

  package_folders = [
    # "process_2019-09-09_22-14-56-0.37",  # vanilla chart 8-15-2019: -0.004937923352530126,
    # "process_2019-09-14_11-47-31-845.11",  # vol_eq
    # "process_2019-09-15_22-19-18-716.64",  # voleq_rec
    # "process_2019-09-20_16-33-46-522.36",  # 8-23 -0.0200  (without volatilty min)
    # "process_2019-09-20_15-07-42-224.54",  # 8-30  / 0.00528 (without volatilty min)
    # "process_2019-09-15_17-14-26-988.57",  # 8-30 .0026 / 0.00392 / .00572
    # "process_2019-09-15_17-10-30-37.45", # 8-29 .0030 / .00389 / 0.0077
    # "process_2019-09-15_17-06-36-672.94",  # 8-28 .0050  / .00441 / 0.0064
    # "process_2019-09-15_17-03-04-8.06", # 8-27 -0.0019 / .00324 / 0.0024
    # "process_2019-09-15_14-02-07-600.9",  # 8-23  -0.0120 / -0.0104 / -0.0174
    # "process_2019-09-15_13-58-15-340.5",  # 8-22   -0.0018 / -0.0004 / 0.0015
    # "process_2019-09-15_13-54-19-578.46",  # 8-21   0.0023 / 0.0049 / 0.01739
    # "process_2019-09-15_13-50-10-391.36",  # 8-20  -0.00198 / -0.0008 / -0.0021
    # "process_2019-09-15_13-45-52-578.17",  # 8-19   0.0063 / 0.0230 / 0.01038
    # "process_2019-09-15_13-39-32-870.98",  # 8-14  -0.0150 / -0.0142 / -0.0120
    # "process_2019-09-15_16-59-07-574.45",  # 8-09 -0.0038 / -0.0018 / -0.0014
    # "process_2019-09-15_16-55-31-225.99", # 8-08  0.0052 / 0.0069 / 0.0107
    # "process_2019-09-15_16-51-55-456.24",  # 8-07 -0.00094 / 0.00087 / 0.0014
    # "process_2019-09-15_16-48-14-245.38",  # 8-06 0.0063 / 0.0081 / 0.0102
    # "process_2019-09-15_16-44-44-97.67",  # 8-05 -0.0196 / -0.01789 / -0.0260
    # "process_2019-09-15_16-41-08-259.13",  # 8-02 -0.0057 / -0.0044 / -0.0009
    # "process_2019-09-15_16-37-38-928.1",  # 8-01 -0.0021 / -.000510995 / .000078447
    # "process_2019-09-15_16-34-06-17.38",  # 7-31 -0.00096 / 0.00187 /  0.0045
    # "process_2019-09-15_16-30-25-885.47",  # 7-30 0.0015 / 0.00236 / 0.0039
    # "process_2019-09-15_16-26-47-851.12",  # 7-29 -0.0018 / 0.000798 / 0.0006
    # "process_2019-09-15_16-23-03-394.13", # 7-26 0.00468 / 0.00422 / 0.00619
    # "process_2019-09-15_16-19-05-587.39",  # 7-25 -0.0062 / -0.00266 / 0.0023
    # "process_2019-09-15_16-15-23-25.45",  # 7-24  0.0022 / 0.00299 /  0.00051
    # "process_2019-09-15_16-11-40-753.51",  # 7-23  0.00349 / 0.00408 / 0.00577
    # "process_2019-09-15_16-07-42-827.66",  # 7-22  0.0015 / 0.01208 / 0.0267
    # "process_2019-09-15_16-04-00-588.58",  # 7-19  -0.0027 / -0.00149 / 0.0052
    # "process_2019-09-15_16-00-11-48.41",  # 7-18  -0.0012 / -0.00095 /  0.0039
    # "process_2019-09-15_15-56-00-356.66",  # 7-17  -0.0022 / -0.0011 / 0
    # "process_2019-09-29_13-18-36-138.66", # fiddy with 19
    # "process_2019-10-05_20-26-42-487.36", # mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
    # "process_2019-09-08_06-08-31-603.66", # 8-15-2019 24h multi3 57.5%
    # "process_2019-08-06_22-46-56-230.65", # vanilla chart
    # "process_2019-09-18_22-12-00-332.17", # voleq 06-18 66%
    # "process_2019-10-06_17-26-47-73.01", # tiny; smid: "ICN323980058926307012"; sgf: .01; mp: 5.0; st: .88; std_min: 9999999; roi: -0.0029
    # "process_2019-10-07_22-13-33-758.15", # tin big; limited set; 196/500; st: .50; std_min: 999999; roi: .00142
    # "process_2019-10-07_22-25-00-943.35", # milleville:,
    # "process_2019-10-09_20-43-50-891.13", # big_name:
  ]

  # "process_2019-10-07_22-13-33-758.15",
  #   tin big; ICN1872651771008733456; 1861/3000; st: .50; std_min: 999999; roi: 0.000286
  # "process_2019-10-05_20-26-42-487.36",  # mp: 5.0; mv: 2.79; sgp: 2.0; mv: 100000
  #   sought_gain_frac = .01; score_threshold = .50; std_min = 2.79; min_price = 10.00; min_volume = 100000: roi: .125% - seems random
  # "process_2019-09-29_13-18-36-138.66",
  #   fiddy with 19 stdev
  #     sm = 5.0:, v: 100k. mp:5.0; roi:
  #     sm = 5.0:, v: 100k. mp:5.0; roi: -.00047
  #     sm: 4; roi: 0.00032
  #     2385/10000; sm: 19.0; st: .6; roi: .00044
  #     775/10000; sm: ; st: .6; roi: 0.00115
  #     63/10000; st: .5; sm: 4.0 roi: 0.0018
  #   fiddy with 19 stdev sgf = .01; st = .50; std_min = 5.0; mp = 20.00; min_volume = 100000: 278/10000; roi: .0014-.00187
  # "process_2019-10-07_22-13-33-758.15",
  #   tin big; limited set;
  #     st: 194/500; st: .50; std_min: 999999; roi: 0.0012
  #     st: 16/500; st: .60; std_min: 999999; roi: 0.0047
  #   tin big II; limited set; (nova_calculator))
  #     st: 56: stm: 10; 264/1000; roi: .00023
  #     st: .50; 280/1000; stm: 9999999; roi: .000758
  #     st: .70; 82/1000; stm: 9999999: roi: .00145;
  #     st: .80; 16/1000; stm: 9999999: roi: .0060;
  # "process_2019-10-07_22-25-00-943.35":
  #   millville: ICN2140008179580940274:
  #     1481/10000; mp: 5.0; st: .5; stdmin: 9999999; roi: -0.00043
  #     618/10000: mp: 5.0; st: .52; stdmin: 7.0; roi: -.0020
  #     904/10000: mp: 5.0; st: .50; stdmin: 8.0; roi: 0.00087
  #     1044/10000: mp: 5.0; st: .50; stdmin: 10.0; roi: .0014
  #     1281/10000: mp: 5.0; st: .50; stdmin: 19.0; roi: -0.0507%
  #     496/10000: mp: 5.0; st: .54; stdmin: 9999999.0; roi: -.0020
  #     11/10000: mp: 5.0; st: .58; stdmin: 9999999.0; roi:  0.0015
  #     38/10000: mp: 5.0; st: .57; stdmin: 9999999.0; roi: -.000073
  #   big_name: ICN1615151565178573620:
  #     1647/2500: sym_foc: see list; sgf: .01; st: .5; stdmin: 9999999; roi: .00048
  #     1297/2500: sym_foc: see list; sgf: .01; st: .5; stdmin: 19; roi: .0005
  #     383/2500: sym_foc: see list; sgf: .01; st: .5; stdmin: 5.0; roi: .00271
  #     153/2500: sym_foc: see list; sgf: .01; st: .5; stdmin: 2.0; roi: .0037
  #     1413/2500: sym_foc: see list; sgf: .01; st: .6; stdmin: 9999999; roi: .0002
  #     1075/2500: sym_foc: see list; sgf: .01; st: .7; stdmin: 9999999; roi: .00019
  #     631/2500: sym_foc: see list; sgf: .01; st: .8; stdmin: 9999999; roi: .00095
  #     172/2500: sym_foc: see list; sgf: .01; st: .9; stdmin: 9999999; roi: .00028
  #   process_2019-10-12_12-46-47-206.17
  #     hundred top volume: ICN8142700079494692925,
  #       670/5000 sgf: .01; st: .56; sm: 99999999; roi: .000982
  #       244/5000 sgf: .01; st: .62; sm: 99999999; roi: -.0046
  #       72/5000 sgf: .01; st: .70; sm: 99999999; roi: .002576
  #   process_2019-10-12_12-46-47-206.17
  #     M&A: ICN8142700079494692925
  #       265/300: sgf: .01; st: .5; sm: 99999999; roi: .0016
  #       263/300: sgf: .01; st: .56; sm: 99999999; roi: .0016
  #       261/300: sgf: .01; st: .70; sm: 99999999; roi: .0016
  #       221/300: sgf: .01; st: .80; sm: 99999999; roi: .0011
  #       155/300: sgf: .01; st: .90; sm: 99999999; roi: .0019
  #       81/300: sgf: .01; st: .95; sm: 99999999; roi: .002
  #       20/300: sgf: .01; st: .95; sm: 99999999; roi: .00379
  #   process_2019-10-12_22-20-18-789.79
  #     2hun: ICN5946595464432340791
  #       sgf: .01; st: .5; sm: 99999999; roi: .000075
  #       x/12000; sgf: .01; st: .6; sm: 99999999; roi: -0.0011
  #       221/3000; sgf: .01; st: .65; sm: 99999999; roi: -0.1691%
  #       44/6000; sgf: .01; st: .75; sm: 99999999; roi: 0.00138
  #       256/XXX; sgf: .01; st: .70; sm: 19; roi: -0.00065
  #   process_2019-10-13_09-11-28-42.6
  #     SU: ICN5946595464432340791
  #       272/300: sgf: .01; st: .5; sm: 99999999; roi: .0002
  #       269/300: sgf: .01; st: .6; sm: 99999999; roi: .00021
  #       204/300: sgf: .01; st: .7; sm: 99999999; roi: .00011
  #       76/300: sgf: .01; st: .8; sm: 99999999; roi: -.00239
  #     SU II: ICN1191201680931978153
  #       272/300: sgf: .01; st: .5; sm: 99999999; roi: .000231
  #       269/300: sgf: .01; st: .6; sm: 99999999; roi: .00021
  #       204/300: sgf: .01; st: .7; sm: 99999999; roi: .0011
  #       76/300: sgf: .01; st: .8; sm: 99999999; roi: -0.00239
  #       22/300: sgf: .01; st: .85; sm: 99999999; roi: -0.00064
  #   process_2019-10-13_12-37-13-166.13
  #     tree hund: ICN8933492435891303237
  #       2371/3000: sgf: .01; st: .5; sm: 99999999; roi: -.0000317
  #       1318/3000: sgf: .01; st: .54; sm: 99999999; roi: 0.000179
  #       /3000: sgf: .01; st: .56; sm: 99999999; roi: 0.00
  #     tree hund II: ICN8933492435891303237
  #       2429/3000: sgf: .01; st: .5; sm: 99999999; roi: -0.00028
  #       1964/3000: sgf: .01; st: .56; sm: 99999999; roi: -0.00017
  #       1242/3000: sgf: .01; st: .62; sm: 99999999; roi: 0.00053
  #       495/3000: sgf: .01; st: .70; sm: 99999999; roi: .000083
  #       /3000: sgf: .01; st: .75; sm: 99999999; roi: -0.000775
  #     tree re II: 	ICN6764335105123795007
  #       1537/3000: sgf: .01; st: .60; sm: 99999999; roi: 0.000545
  #       521/3000: sgf: .01; st: .7; sm: 99999999; roi: -0.00069
  #       24/3000: sgf: .01; st: .7; sm: 99999999; roi: 0.001447
  #       ?/3000: sgf: .01; st: .8; sm: 99999999; roi: -0.00159
  #   process_2019-10-13_12-37-13-166.13
  #       tree re III:	ICN276799418659461897
  #         /3000: sgf: .01; st: .5; sm: 99999999; roi: .00
  #   process_2019-10-15_08-57-11-1.53
  #       tree re III:	ICN276799418659461897
  #         1835/3000: sgf: .01; st: .5; sm: 99999999; roi: .000289
  #         691/3000: sgf: .01; st: .92; sm: 99999999; roi: -.00055
  #         92/3000: sgf: .01; st: .93; sm: 99999999; roi: .001855
  #         0/3000: sgf: .01; st: .94; sm: 99999999; roi: 0
  #         0/3000: sgf: .01; st: .935; sm: 99999999; roi: 0
  #         14/3000: sgf: .01; st: .9325; sm: 99999999; roi: 0.000145
  #         /3000: sgf: .01; st: .92; sm: 19; roi: .000
  #   process_2019-10-16_23-20-33-56.52
  #       five huns I:	ICN4590655548075425593
  #         2599/5000: sgf: .01; st: .6; sm: 99999999; roi: -0.00045
  #         ?/5000: sgf: .01; st: .7; sm: 99999999; roi: -0.000457
  #         258/5000: sgf: .01; st: .75; sm: 99999999; roi: 0.00065
  #         11/5000: sgf: .01; st: .8; sm: 99999999; roi: 0.0011
  #       five huns II:	ICN8519906956010317119
  #         ?/3000: sgf: .01; st: .5; sm: 99999999; roi: -0.00051
  #         306/3000: sgf: .01; st: .6; sm: 99999999; roi: 0.0011
  #         212/3000: sgf: .01; st: .65; sm: 99999999; roi: 0.0007
  #         182/3000: sgf: .01; st: .7; sm: 99999999; roi: 0.00017
  #       fun huns IV: ICN3063105254453531854
  #         ?/5000: sgf: .01; st: .5; sm: 99999999; roi: 0.00
  #   process_2019-10-18_19-04-56-485.32
  #       NetTes: ICN5121459976749002043
  #         210/300: sgf: .01; st: .5; sm: 99999999; roi: -0.00075
  #         32/300: sgf: .01; st: .85; sm: 99999999; roi: 0.00135
  #         18/300: sgf: .01; st: .87; sm: 99999999; roi: 0.0005
  #   process_2019-10-18_19-35-49-331.29
  #       WGOO: ICN5121459976749002043
  #         161/300: sgf: .01; st: .5; sm: 99999999; roi: -0.00035
  #         100/300: sgf: .01; st: .7; sm: 99999999; roi: 0.00072
  #         62/300: sgf: .01; st: .8; sm: 99999999; roi: 0.00112
  #         47/300: sgf: .01; st: .85; sm: 99999999; roi: 0.000825
  #         23/300: sgf: .01; st: .9; sm: 99999999; roi: 0.00246
  #   process_2019-10-19_09-09-37-357.69
  #       FBNV: ICN2127738899220417151
  #         120/300: sgf: .01; st: .5; sm: 99999999; roi: 0.00014 / -0.00031
  #         63/300: sgf: .01; st: .53; sm: 99999999; roi: 0.0001611
  #         12/300: sgf: .01; st: .55; sm: 99999999; roi: -0.0001611
  #   process_2019-10-19_09-17-10-340.29
  #     # SEV_BIG: ICN4535143846407309133; End Date: "2018-08-02"; ['BAC', 'EXEL', 'F', 'GE', 'RNVA', 'SDRL1', 'WFC']
  #       534/706: sgf: .01; st: .5; sm: 99999999; roi: 0.000279
  #       440/706: sgf: .01; st: .55; sm: 99999999; roi: 0.000051
  #       282/706: sgf: .01; st: .6; sm: 99999999; roi: -0.000055
  #       62/706: sgf: .01; st: .65; sm: 99999999; roi: -0.0006102
  #     # SevBig II: ICN7054239439526564691
  #       9/706: sgf: .01; st: .5; sm: 99999999; roi: 0.0034 (bail@gaplow)
  #     bail@gaplow
  #       21/706: sgf: .01; st: .67; sm: 99999999; roi: 0.00100; || 62/705; .65: -0.0095%; 282/705; .6; -0.0011; || /705; .65;
  #    process_2019-10-19_22-34-43-919.88
  #     # SevBig ICN208523049317614530 End Date: 2019-01-15 ['AMD', 'BAC', 'BRPA', 'F', 'FB', 'GE', 'PRTH']
  #       472/552: sgf: .01; st: .5; sm: 99999999; roi: 0.00043
  #       278/552: sgf: .01; st: .6; sm: 99999999; roi: 0.0044
  #       278/552 sgf: .01; st: .85; sm: 99999999; roi: .00146 (nova)
  #    process_2019-10-20_19-42-48-304.63
  #     top_hun: ICN6800560436346434938
  #       1575/3000: sgf: .01; st: .5; sm: 99999999; roi: .00078
  #       2858/8800: sgf: .01; st: .6; sm: 99999999; roi: .000955
  #       556/3000: .01; st: .7; sm: 99999999; roi: 0.0018
  #       87/3000: .01; st: .8; sm: 99999999; roi: .00522


  tuples = [
    # ("ICN8142700079494692925", "process_2019-10-12_12-46-47-206.17"), # hundred
    # ("ICN8748316622033513158", "process_2019-09-29_13-18-36-138.66") # fiddy
    # ("ICN1872651771008733456", "process_2019-10-07_22-13-33-758.15") # tin big;
    # ("ICN7012160376262305340", "process_2019-10-07_22-13-33-758.15"), # tin big II;
    # ("ICN1615151565178573620", "process_2019-10-09_20-43-50-891.13"), # big_name:
    # ("ICN2140008179580940274", "process_2019-10-07_22-25-00-943.35"),  # milleville I,
    # ("ICN7008377799369496303", "process_2019-10-12_22-13-46-43.59") # M&A:
    # ("ICN8142700079494692925", "process_2019-10-12_12-46-47-206.17") # hun_top
    # ("ICN5946595464432340791", "process_2019-10-12_22-20-18-789.79"),  # 2hun:
    # ("ICN4022776603280292215", "process_2019-10-13_09-11-28-42.6")  # su:
    # ("ICN1191201680931978153", "process_2019-10-13_09-11-28-42.6")  # su II:
    # ("ICN8933492435891303237", "process_2019-10-13_12-37-13-166.13") # tree:
    # ("ICN64706518165040384", "process_2019-10-13_12-37-13-166.13")  # tree II:
    # ("ICN6764335105123795007", "process_2019-10-13_12-37-13-166.13"), # tree re II
    # ("ICN276799418659461897", "process_2019-10-13_12-37-13-166.13")  # tree re III with tree folder
    # ("ICN276799418659461897", "process_2019-10-15_08-57-11-1.53")  # tree re III with correct folder
    # ("ICN4590655548075425593", "process_2019-10-16_23-20-33-56.52")  # five huns I
    # ("ICN8519906956010317119", "process_2019-10-16_23-20-33-56.52")  # five huns II
    # ("ICN3063105254453531854", "process_2019-10-16_23-20-33-56.52")  # five huns IV
    # ("ICN5121459976749002043", "process_2019-10-18_19-04-56-485.32") # NetTes
    # ("ICN3310805515309381452", "process_2019-10-18_19-35-49-331.29") # WGOO
    # ("ICN2127738899220417151", "process_2019-10-19_09-09-37-357.69") # FBNV
    # ("ICN4535143846407309133", "process_2019-10-19_09-17-10-340.29") # SevBig
    # ("ICN7054239439526564691", "process_2019-10-19_09-17-10-340.29") # SevBig II
    # ("ICN208523049317614530", "process_2019-10-19_22-34-43-919.88") # SevBig 2019-01-15
    ("ICN6800560436346434938", "process_2019-10-20_19-42-48-304.63") # top_hun
  ]

  results = []
  for short_model_id, pf in tuples:
    roi = pred_images_from_folder(short_model_id=short_model_id, package_folder=pf)
    results.append({'package_folder': pf, 'roi': roi})

  rois = []
  for r in results:
    rois.append(r['roi'])
    logger.info(f"{r['package_folder']}; roi: {r['roi']}")

  logger.info(f"Mean ROI: {round(statistics.mean(rois) * 100, 4)}%")


def pred_images_from_folder(short_model_id, package_folder):
  # Arrange
  data_cache_dir = os.path.join(config.constants.APP_FIN_OUTPUT_DIR, "selection_packages", "SelectChartZipUploadService", package_folder)
  image_dir = os.path.join(data_cache_dir, "test_holdout")
  # image_dir = os.path.join(data_cache_dir, "graphed")

  sought_gain_frac = .01
  min_price = 5.00
  score_threshold = .80
  std_min = 9999999999999999.0
  min_volume = 1000
  max_files = 3000
  start_sample_date = None # date_utils.parse_std_datestring("2019-03-05")

  purge_cached = False

  auto_ml_service = AutoMlPredictionService(short_model_id, package_dir=data_cache_dir, score_threshold=score_threshold)

  # Act
  stopwatch = Stopwatch()
  stopwatch.start()
  roi, symbol_info = auto_ml_service.predict_and_calculate(task_dir=package_folder, image_dir=image_dir,
                                              min_price=min_price, min_volume=min_volume,
                                              sought_gain_frac=sought_gain_frac, std_min=std_min,
                                              max_files=max_files, purge_cached=purge_cached, start_sample_date=start_sample_date)
  stopwatch.stop()

  logger.info(f"Elapsed time: {round(stopwatch.duration / 60, 2)} minutes")

  return roi

if __name__ == "__main__":
  predict_and_calculate()