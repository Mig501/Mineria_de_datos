
import socket
import time
import json
from datetime import datetime, timezone, timedelta


DATA = {
  "BBVA.MC": [
 {
      "price": 15.695,
      "high": 15.685,
      "low": 16.315,
      "volume": 9221
    },
    {
      "price": 16.28,
      "high": 16.36,
      "low": 16.24,
      "volume": 12212
    },
    {
      "price": 16.23,
      "high": 16.31,
      "low": 16.18,
      "volume": 10005
    },
    {
      "price": 16.17,
      "high": 16.27,
      "low": 16.13,
      "volume": 14770
    },
    {
      "price": 16.12,
      "high": 16.22,
      "low": 16.08,
      "volume": 15966
    },
    {
      "price": 16.06,
      "high": 16.16,
      "low": 16.02,
      "volume": 10574
    },
    {
      "price": 16.11,
      "high": 16.16,
      "low": 16.0,
      "volume": 9811
    },
    {
      "price": 16.05,
      "high": 16.15,
      "low": 16.01,
      "volume": 16411
    },
    {
      "price": 15.98,
      "high": 16.09,
      "low": 15.94,
      "volume": 22339
    },
    {
      "price": 16.0,
      "high": 16.05,
      "low": 15.93,
      "volume": 9659
    },
    {
      "price": 15.95,
      "high": 16.04,
      "low": 15.91,
      "volume": 17302
    },
    {
      "price": 15.9,
      "high": 15.99,
      "low": 15.86,
      "volume": 17470
    },
    {
      "price": 15.86,
      "high": 15.94,
      "low": 15.82,
      "volume": 18042
    },
    {
      "price": 15.82,
      "high": 15.9,
      "low": 15.78,
      "volume": 17915
    },
    {
      "price": 15.78,
      "high": 15.86,
      "low": 15.74,
      "volume": 18946
    },
    {
      "price": 15.74,
      "high": 15.82,
      "low": 15.7,
      "volume": 10595
    },
    {
      "price": 15.7,
      "high": 15.78,
      "low": 15.66,
      "volume": 11963
    },
    {
      "price": 15.66,
      "high": 15.74,
      "low": 15.62,
      "volume": 17162
    },
    {
      "price": 15.62,
      "high": 15.7,
      "low": 15.58,
      "volume": 8445
    },
    {
      "price": 15.52,
      "high": 15.66,
      "low": 15.48,
      "volume": 9777
    },
    {
      "price": 15.215,
      "high": 15.56,
      "low": 15.18,
      "volume": 6692
    },
    {
      "price": 15.385,
      "high": 15.435,
      "low": 15.215,
      "volume": 16144
    },
    {
      "price": 15.385,
      "high": 15.435,
      "low": 15.215,
      "volume": 16144
    }
  ],
  "SAB.MC": [
    {
      "price": 3.375,
      "high": 3.373,
      "low": 3.517,
      "volume": 2717
    },
    {
      "price": 3.52,
      "high": 3.54,
      "low": 3.48,
      "volume": 3505
    },
    {
      "price": 3.49,
      "high": 3.52,
      "low": 3.46,
      "volume": 6910
    },
    {
      "price": 3.47,
      "high": 3.505,
      "low": 3.44,
      "volume": 10265
    },
    {
      "price": 3.455,
      "high": 3.49,
      "low": 3.43,
      "volume": 8692
    },
    {
      "price": 3.44,
      "high": 3.47,
      "low": 3.415,
      "volume": 7841
    },
    {
      "price": 3.43,
      "high": 3.455,
      "low": 3.4,
      "volume": 3155
    },
    {
      "price": 3.41,
      "high": 3.44,
      "low": 3.39,
      "volume": 10316
    },
    {
      "price": 3.395,
      "high": 3.43,
      "low": 3.38,
      "volume": 4435
    },
    {
      "price": 3.385,
      "high": 3.415,
      "low": 3.37,
      "volume": 6706
    },
    {
      "price": 3.37,
      "high": 3.405,
      "low": 3.35,
      "volume": 10463
    },
    {
      "price": 3.36,
      "high": 3.395,
      "low": 3.34,
      "volume": 7096
    },
    {
      "price": 3.35,
      "high": 3.38,
      "low": 3.33,
      "volume": 4230
    },
    {
      "price": 3.34,
      "high": 3.37,
      "low": 3.32,
      "volume": 5698
    },
    {
      "price": 3.33,
      "high": 3.36,
      "low": 3.31,
      "volume": 4736
    },
    {
      "price": 3.325,
      "high": 3.35,
      "low": 3.305,
      "volume": 5955
    },
    {
      "price": 3.32,
      "high": 3.345,
      "low": 3.3,
      "volume": 2185
    },
    {
      "price": 3.315,
      "high": 3.34,
      "low": 3.295,
      "volume": 2166
    },
    {
      "price": 3.31,
      "high": 3.335,
      "low": 3.29,
      "volume": 1498
    },
    {
      "price": 3.305,
      "high": 3.33,
      "low": 3.285,
      "volume": 1889
    },
    {
      "price": 3.302,
      "high": 3.325,
      "low": 3.282,
      "volume": 2654
    },
    {
      "price": 3.311,
      "high": 3.276,
      "low": 3.278,
      "volume": 7640
    },
    {
      "price": 3.311,
      "high": 3.276,
      "low": 3.278,
      "volume": 7640
    }
  ],
  "IBE.MC": [
    {
      "price": 16.675,
      "high": 16.665,
      "low": 16.815,
      "volume": 2684
    },
    {
      "price": 16.9,
      "high": 16.96,
      "low": 16.86,
      "volume": 6787
    },
    {
      "price": 16.85,
      "high": 16.9,
      "low": 16.82,
      "volume": 4472
    },
    {
      "price": 16.8,
      "high": 16.87,
      "low": 16.77,
      "volume": 9590
    },
    {
      "price": 16.76,
      "high": 16.82,
      "low": 16.74,
      "volume": 3554
    },
    {
      "price": 16.72,
      "high": 16.78,
      "low": 16.7,
      "volume": 10600
    },
    {
      "price": 16.68,
      "high": 16.74,
      "low": 16.66,
      "volume": 5210
    },
    {
      "price": 16.64,
      "high": 16.7,
      "low": 16.62,
      "volume": 8254
    },
    {
      "price": 16.6,
      "high": 16.66,
      "low": 16.58,
      "volume": 7686
    },
    {
      "price": 16.56,
      "high": 16.62,
      "low": 16.54,
      "volume": 2832
    },
    {
      "price": 16.52,
      "high": 16.58,
      "low": 16.5,
      "volume": 5093
    },
    {
      "price": 16.48,
      "high": 16.54,
      "low": 16.46,
      "volume": 8104
    },
    {
      "price": 16.44,
      "high": 16.5,
      "low": 16.42,
      "volume": 7954
    },
    {
      "price": 16.4,
      "high": 16.46,
      "low": 16.38,
      "volume": 4984
    },
    {
      "price": 16.36,
      "high": 16.42,
      "low": 16.34,
      "volume": 2590
    },
    {
      "price": 16.32,
      "high": 16.38,
      "low": 16.3,
      "volume": 4142
    },
    {
      "price": "null",
      "high": "null",
      "low": "null",
      "volume": "null"
    },
    {
      "price": 16.28,
      "high": 16.34,
      "low": 16.26,
      "volume": 8259
    },
    {
      "price": 16.24,
      "high": 16.3,
      "low": 16.22,
      "volume": 11126
    },
    {
      "price": 16.22,
      "high": 16.26,
      "low": 16.2,
      "volume": 2235
    },
    {
      "price": 16.2,
      "high": 16.24,
      "low": 16.18,
      "volume": 7038
    },
    {
      "price": 16.19,
      "high": 16.23,
      "low": 16.17,
      "volume": 4097
    },
    {
      "price": 16.105,
      "high": 16.025,
      "low": 16.125,
      "volume": 4816
    },
    {
      "price": 16.105,
      "high": 16.025,
      "low": 16.125,
      "volume": 4816
    }
  ],
  "NTGY.MC": [
    {
      "price": 26.040001,
      "high": 26.040001,
      "low": 26.24,
      "volume": 1500
    },
    {
      "price": 26.22,
      "high": 26.28,
      "low": 26.18,
      "volume": 6136
    },
    {
      "price": 26.14,
      "high": 26.2,
      "low": 26.1,
      "volume": 4632
    },
    {
      "price": 26.08,
      "high": 26.14,
      "low": 26.04,
      "volume": 2118
    },
    {
      "price": 26.02,
      "high": 26.08,
      "low": 25.98,
      "volume": 1765
    },
    {
      "price": 25.98,
      "high": 26.04,
      "low": 25.94,
      "volume": 2290
    },
    {
      "price": 25.92,
      "high": 25.98,
      "low": 25.88,
      "volume": 3824
    },
    {
      "price": 25.88,
      "high": 25.94,
      "low": 25.84,
      "volume": 1704
    },
    {
      "price": 25.84,
      "high": 25.9,
      "low": 25.8,
      "volume": 1415
    },
    {
      "price": 25.8,
      "high": 25.86,
      "low": 25.76,
      "volume": 2690
    },
    {
      "price": 25.76,
      "high": 25.82,
      "low": 25.72,
      "volume": 1538
    },
    {
      "price": 25.72,
      "high": 25.780001,
      "low": 25.68,
      "volume": 3270
    },
    {
      "price": 25.700001,
      "high": 25.76,
      "low": 25.66,
      "volume": 1799
    },
    {
      "price": 25.66,
      "high": 25.72,
      "low": 25.62,
      "volume": 1731
    },
    {
      "price": 25.62,
      "high": 25.68,
      "low": 25.58,
      "volume": 2682
    },
    {
      "price": 25.58,
      "high": 25.66,
      "low": 25.54,
      "volume": 1305
    },
    {
      "price": 25.54,
      "high": 25.62,
      "low": 25.5,
      "volume": 1449
    },
    {
      "price": 25.52,
      "high": 25.58,
      "low": 25.48,
      "volume": 1363
    },
    {
      "price": 25.5,
      "high": 25.56,
      "low": 25.46,
      "volume": 1469
    },
    {
      "price": 25.46,
      "high": 25.52,
      "low": 25.44,
      "volume": 2865
    },
    {
      "price": 25.42,
      "high": 25.48,
      "low": 25.4,
      "volume": 3269
    },
    {
      "price": 25.280001,
      "high": 25.26,
      "low": 25.280001,
      "volume": 2570
    },
    {
      "price": 25.280001,
      "high": 25.26,
      "low": 25.280001,
      "volume": 2570
    }
  ],
  "TEF.MC": [
    {
      "price": 4.405,
      "high": 4.403,
      "low": 4.47,
      "volume": 9372
    },
    {
      "price": 4.495,
      "high": 4.52,
      "low": 4.48,
      "volume": 18033
    },
    {
      "price": 4.485,
      "high": 4.505,
      "low": 4.47,
      "volume": 13394
    },
    {
      "price": 4.475,
      "high": 4.495,
      "low": 4.46,
      "volume": 8626
    },
    {
      "price": 4.47,
      "high": 4.485,
      "low": 4.45,
      "volume": 7535
    },
    {
      "price": 4.46,
      "high": 4.475,
      "low": 4.44,
      "volume": 7491
    },
    {
      "price": 4.45,
      "high": 4.465,
      "low": 4.43,
      "volume": 9107
    },
    {
      "price": 4.44,
      "high": 4.455,
      "low": 4.42,
      "volume": 4426
    },
    {
      "price": 4.43,
      "high": 4.445,
      "low": 4.41,
      "volume": 5387
    },
    {
      "price": 4.42,
      "high": 4.435,
      "low": 4.4,
      "volume": 7222
    },
    {
      "price": 4.41,
      "high": 4.425,
      "low": 4.39,
      "volume": 12312
    },
    {
      "price": 4.405,
      "high": 4.42,
      "low": 4.385,
      "volume": 11314
    },
    {
      "price": 4.395,
      "high": 4.41,
      "low": 4.375,
      "volume": 5069
    },
    {
      "price": 4.385,
      "high": 4.4,
      "low": 4.365,
      "volume": 6653
    },
    {
      "price": 4.375,
      "high": 4.39,
      "low": 4.355,
      "volume": 9094
    },
    {
      "price": 4.365,
      "high": 4.38,
      "low": 4.345,
      "volume": 6611
    },
    {
      "price": 4.355,
      "high": 4.37,
      "low": 4.335,
      "volume": 15579
    },
    {
      "price": 4.345,
      "high": 4.36,
      "low": 4.325,
      "volume": 6080
    },
    {
      "price": 4.335,
      "high": 4.35,
      "low": 4.315,
      "volume": 12555
    },
    {
      "price": 4.325,
      "high": 4.34,
      "low": 4.305,
      "volume": 6885
    },
    {
      "price": 4.315,
      "high": 4.33,
      "low": 4.295,
      "volume": 9350
    },
    {
      "price": 4.288,
      "high": 4.278,
      "low": 4.283,
      "volume": 9986
    },
    {
      "price": 4.288,
      "high": 4.278,
      "low": 4.283,
      "volume": 9986
    }
  ],
  "CLNX.MC": [
    {
      "price": 29.549999,
      "high": 29.119999,
      "low": 29.32,
      "volume": 948
    },
    {
      "price": 29.900002,
      "high": 29.98,
      "low": 29.860001,
      "volume": 2792
    },
    {
      "price": 29.82,
      "high": 29.900002,
      "low": 29.780001,
      "volume": 1179
    },
    {
      "price": 29.740002,
      "high": 29.82,
      "low": 29.700001,
      "volume": 2658
    },
    {
      "price": 29.68,
      "high": 29.759998,
      "low": 29.639999,
      "volume": 1914
    },
    {
      "price": 29.610001,
      "high": 29.68,
      "low": 29.560001,
      "volume": 2166
    },
    {
      "price": 29.540001,
      "high": 29.610001,
      "low": 29.5,
      "volume": 3002
    },
    {
      "price": 29.470001,
      "high": 29.540001,
      "low": 29.43,
      "volume": 2826
    },
    {
      "price": 29.400002,
      "high": 29.48,
      "low": 29.360001,
      "volume": 1872
    },
    {
      "price": 29.330002,
      "high": 29.409999,
      "low": 29.290001,
      "volume": 802
    },
    {
      "price": 29.259998,
      "high": 29.34,
      "low": 29.220001,
      "volume": 1266
    },
    {
      "price": 29.189999,
      "high": 29.27,
      "low": 29.150002,
      "volume": 1064
    },
    {
      "price": 29.12,
      "high": 29.200001,
      "low": 29.080002,
      "volume": 2806
    },
    {
      "price": 29.049999,
      "high": 29.130001,
      "low": 29.009998,
      "volume": 1773
    },
    {
      "price": 28.98,
      "high": 29.040001,
      "low": 28.939999,
      "volume": 1154
    },
    {
      "price": 28.909999,
      "high": 28.970001,
      "low": 28.869999,
      "volume": 2934
    },
    {
      "price": 28.84,
      "high": 28.900002,
      "low": 28.799999,
      "volume": 1200
    },
    {
      "price": 28.77,
      "high": 28.830002,
      "low": 28.73,
      "volume": 2362
    },
    {
      "price": 28.700001,
      "high": 28.760002,
      "low": 28.66,
      "volume": 1915
    },
    {
      "price": 28.630001,
      "high": 28.689999,
      "low": 28.59,
      "volume": 2355
    },
    {
      "price": 28.18,
      "high": 28.240002,
      "low": 28.16,
      "volume": 3962
    },
    {
      "price": 27.990002,
      "high": 27.77,
      "low": 28.0,
      "volume": 3245
    },
    {
      "price": 27.990002,
      "high": 27.77,
      "low": 28.0,
      "volume": 3245
    }
  ],
  "CABK.MC": [
    {
      "price": 8.562,
      "high": 8.552,
      "low": 8.808,
      "volume": 2740
    },
    {
      "price": 8.762,
      "high": 8.83,
      "low": 8.76,
      "volume": 8305
    },
    {
      "price": 8.74,
      "high": 8.8,
      "low": 8.73,
      "volume": 7034
    },
    {
      "price": 8.705,
      "high": 8.76,
      "low": 8.7,
      "volume": 11057
    },
    {
      "price": 8.69,
      "high": 8.74,
      "low": 8.68,
      "volume": 6678
    },
    {
      "price": 8.66,
      "high": 8.72,
      "low": 8.65,
      "volume": 11003
    },
    {
      "price": 8.635,
      "high": 8.69,
      "low": 8.63,
      "volume": 8174
    },
    {
      "price": 8.605,
      "high": 8.66,
      "low": 8.6,
      "volume": 8256
    },
    {
      "price": 8.59,
      "high": 8.64,
      "low": 8.58,
      "volume": 5174
    },
    {
      "price": 8.555,
      "high": 8.61,
      "low": 8.55,
      "volume": 11108
    },
    {
      "price": 8.535,
      "high": 8.59,
      "low": 8.53,
      "volume": 3624
    },
    {
      "price": 8.505,
      "high": 8.56,
      "low": 8.5,
      "volume": 4342
    },
    {
      "price": 8.485,
      "high": 8.54,
      "low": 8.48,
      "volume": 9270
    },
    {
      "price": 8.455,
      "high": 8.51,
      "low": 8.45,
      "volume": 8829
    },
    {
      "price": 8.43,
      "high": 8.48,
      "low": 8.42,
      "volume": 7523
    },
    {
      "price": 8.405,
      "high": 8.46,
      "low": 8.4,
      "volume": 7590
    },
    {
      "price": 8.375,
      "high": 8.43,
      "low": 8.37,
      "volume": 4424
    },
    {
      "price": 8.35,
      "high": 8.4,
      "low": 8.34,
      "volume": 8986
    },
    {
      "price": 8.325,
      "high": 8.38,
      "low": 8.32,
      "volume": 10861
    },
    {
      "price": 8.305,
      "high": 8.35,
      "low": 8.3,
      "volume": 5832
    },
    {
      "price": 8.285,
      "high": 8.33,
      "low": 8.28,
      "volume": 3890
    },
    {
      "price": 8.266,
      "high": 8.204,
      "low": 8.204,
      "volume": 5163
    },
    {
      "price": 8.266,
      "high": 8.204,
      "low": 8.204,
      "volume": 5163
    }
  ],
  "BKT.MC": [
    {
      "price": 13.252,
      "high": 13.24,
      "low": 13.6,
      "volume": 2622
    },
    {
      "price": 13.552,
      "high": 13.6,
      "low": 13.52,
      "volume": 9280
    },
    {
      "price": 13.526,
      "high": 13.56,
      "low": 13.48,
      "volume": 1901
    },
    {
      "price": 13.47,
      "high": 13.52,
      "low": 13.44,
      "volume": 3918
    },
    {
      "price": 13.43,
      "high": 13.48,
      "low": 13.4,
      "volume": 6168
    },
    {
      "price": 13.398,
      "high": 13.44,
      "low": 13.36,
      "volume": 7688
    },
    {
      "price": 13.37,
      "high": 13.42,
      "low": 13.32,
      "volume": 6725
    },
    {
      "price": 13.31,
      "high": 13.38,
      "low": 13.28,
      "volume": 5009
    },
    {
      "price": 13.29,
      "high": 13.34,
      "low": 13.24,
      "volume": 2541
    },
    {
      "price": 13.23,
      "high": 13.3,
      "low": 13.2,
      "volume": 7821
    },
    {
      "price": 13.21,
      "high": 13.26,
      "low": 13.16,
      "volume": 6212
    },
    {
      "price": 13.17,
      "high": 13.22,
      "low": 13.12,
      "volume": 2981
    },
    {
      "price": 13.13,
      "high": 13.18,
      "low": 13.08,
      "volume": 3085
    },
    {
      "price": 13.07,
      "high": 13.14,
      "low": 13.04,
      "volume": 10558
    },
    {
      "price": 13.05,
      "high": 13.1,
      "low": 13.0,
      "volume": 2991
    },
    {
      "price": 13.01,
      "high": 13.06,
      "low": 12.96,
      "volume": 7310
    },
    {
      "price": 12.97,
      "high": 13.02,
      "low": 12.92,
      "volume": 3050
    },
    {
      "price": 12.93,
      "high": 12.98,
      "low": 12.88,
      "volume": 1946
    },
    {
      "price": 12.895,
      "high": 12.96,
      "low": 12.86,
      "volume": 5630
    },
    {
      "price": 12.885,
      "high": 12.94,
      "low": 12.84,
      "volume": 4727
    },
    {
      "price": 12.872,
      "high": 12.92,
      "low": 12.83,
      "volume": 4459
    },
    {
      "price": 12.85,
      "high": 12.756,
      "low": 12.758,
      "volume": 5571
    },
    {
      "price": 12.85,
      "high": 12.756,
      "low": 12.758,
      "volume": 5571
    }
  ]
}

HOST = "localhost"
PUERTO = 8080
SLEEP_SECONDS = 3


def filas_totales_sync(rows_by_ticker: dict) -> int:
    if not rows_by_ticker:
        return 0
    return min(len(v) for v in rows_by_ticker.values())

def emitir_por_indice(conn, rows_by_ticker: dict, i: int):
    for ticker, rows in rows_by_ticker.items():
        if i >= len(rows):
            continue
        r = rows[i]
        msg = {
            "ticker": ticker,
            "price": r["price"],
            "high":  r["high"],
            "low":   r["low"],
            "volume":   r["volume"]
        }
        conn.sendall((json.dumps(msg) + "\n").encode("utf-8"))
        time.sleep(0.01)

def enviar_datos(host=HOST, puerto=PUERTO):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, puerto))
        s.listen(1)  #Para que espere la conexion
        print(f"Esperando conexión en {host}:{puerto}...")
        conn, addr = s.accept()
        with conn:
            print(f"Conexión establecida con {addr}")
            n = filas_totales_sync(DATA)
            for i in range(n):
                emitir_por_indice(conn, DATA, i)
                time.sleep(SLEEP_SECONDS)
        print("Conexión cerrada.")

if __name__ == "__main__":
    enviar_datos()
