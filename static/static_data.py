import json
import pandas as pd

class StaticData:
    def __init__(self):
        self.state_tracks = {'tas': ['burnie', 'carrick', 'devonport', 'hobart', 'hobart s', 
                                     'king island', 'launceston', 'new norfolk', 'scottsdale', 
                                     'spreyton', 'st marys'], 

                             'nsw': ['albury', 'armidale', 'bankstown', 'bathurst', 'blayney', 
                                     'broken hill', 'bulli', 'canberra', 'cessnock', 'coolamon', 
                                     'cootamundra', 'cowra', 'dubbo', 'eugowra', 'fairfield', 
                                     'forbes', 'goulburn', 'griffith', 'harold park', 'inverell', 
                                     'junee', 'leeton', 'lithgow', 'maitland', 'menangle', 
                                     'muswellbrook', 'narrabri', 'newcastle', 'nowra', 'orange', 
                           
                                     'orange racecourse', 'parkes', 'peak hill', 'penrith', 
                                     'tamworth', 'temora', 'tweed heads', 
                                     'wagga', 'west wyalong', 'wyong', 'young'], 

                             'wa': ['albany', 'bridgetown', 'bunbury', 'busselton', 'byford', 
                                     'collie', 'cunderdin', 'geraldton', 'gloucester park', 
                                     'harvey', 'kalgoorlie', 'kellerb', 'kellerberrin', 'narrogin', 
                                     'northam', 'pinjarra', 'wagin', 'wanneroo', 'williams', 'york'],

                             'vic': ['ararat', 'bacchus marsh', 'ballarat', 'benalla', 'bendigo', 
                                     'birchip', 'boort', 'charlton', 'cobram', 'cranbourne', 
                                     'donald', 'echuca', 'elmore', 'geelong', 'gunbower', 'hamilton',
                                       'horsham', 'kilmore', 'maryborough', 'melton', 'mildura', 
                                       'moonee valley', 'mooroopna', 'nyah', 'ouyen', 'robinvale', 
                                       'shepparton', 'st arnaud', 'stawell', 'swan hill', 'terang', 
                                       'wangaratta', 'warragul', 'warrnambool', 'wedderburn', 'yarra valley'], 
                                       
                             'sa': ['gawler', 'globe derby', 'kadina', 'kapunda', 'kimba', 
                                    'mount gambier', 'port pirie', 'south australia', 'strathalbyn', 
                                    'victor harbor', 'wayville', 'whyalla'], 
                             
                             'qld': ['albion park', 'beaudesert', 'boonah', 'cliftonshow', 'deagon (grass)', 
                                     'gatton', 'gold coast', 'gympie', 'gympie show', 'kilcoy', 'kngaroy',
                                       'mackay', 'marburg', 'maryborough', 'maryborough (qld)', 'nanango show', 
                                       'redcliffe', 'rna showgrounds', 'rockhampton', 'rocklea', 'toowoomba', 
                                       'toowoomba showground', 'townsville', 'warwick']
                            }

        self.track_name_updates = {
            'tabcorp pk menangle': 'menangle',
            'globe derby park': 'globe derby',
            'lockyer (gatton)': 'gatton',
            'central wheatbelt': 'kellerberrin',
            'riverina paceway': 'wagga'
        }

        self.aus_track_distances = {
                    'albany': [400.0, 1826.0, 1828.0, 1832.0, 2150.0, 2242.0, 2243.0, 2247.0, 2252.0, 
                               2258.0, 2265.0, 2643.0, 2648.0, 2683.0, 2690.0, 3078.0, 3113.0], 
                    'albion park': [1119.0, 1609.0, 1628.0, 1660.0, 2138.0, 2647.0, 2680.0, 3157.0], 
                    'albury': [1750.0, 1770.0, 2150.0, 2170.0, 2550.0, 2555.0, 2570.0, 2950.0, 2970.0], 
                    'ararat': [1790.0, 2165.0, 2190.0, 2195.0, 2570.0, 2590.0, 2975.0], 
                    'armidale': [1609.0, 1980.0, 2360.0], 
                    'bacchus marsh': [1750.0, 2140.0, 2552.0], 
                    'ballarat': [1200.0, 1609.0, 1710.0, 2200.0, 2710.0], 
                    'bankstown': [1305.0, 1335.0, 1740.0, 2140.0, 2160.0, 2540.0], 
                    'bathurst': [1195.0, 1200.0, 1609.0, 1730.0, 1740.0, 2130.0, 2260.0, 2360.0, 
                                 2520.0, 2535.0, 2790.0, 2795.0], 
                    'beaudesert': [1609.0], 
                    'benalla': [1830.0, 2280.0], 
                    'bendigo': [1150.0, 1515.0, 1609.0, 1650.0, 1965.0, 2150.0, 2415.0, 2650.0, 2865.0], 
                    'birchip': [1750.0, 2150.0, 2550.0], 
                    'blayney': [1640.0, 2000.0, 2400.0], 
                    'boonah': [1609.0], 
                    'boort': [1900.0, 1950.0, 2250.0, 2600.0, 2612.0], 
                    'bridgetown': [1784.0, 2190.0, 2544.0, 2597.0], 
                    'broken hill': [1609.0, 1610.0, 1900.0, 2210.0, 2500.0], 
                    'bulli': [1609.0, 1940.0, 2348.0, 3086.0], 
                    'bunbury': [400.0, 1140.0, 1609.0, 2030.0, 2100.0, 2500.0, 2503.0, 2536.0,
                                2550.0, 2568.0, 2569.0, 2990.0], 
                    'burnie': [1577.0, 1892.0, 2180.0, 2500.0, 2789.0, 2798.0], 
                    'busselton': [2030.0, 2400.0, 2680.0], 
                    'byford': [400.0, 1750.0, 2150.0, 2500.0, 2503.0, 2550.0], 
                    'canberra': [1730.0, 1770.0, 1900.0, 2140.0, 2170.0, 2540.0, 2570.0], 
                    'carrick': [1609.0, 1670.0, 1680.0, 2150.0, 2645.0, 2650.0], 
                    'cessnock': [1752.0], 
                    'charlton': [1609.0, 1810.0, 2100.0, 2230.0, 2250.0, 2570.0, 2650.0], 
                    'cliftonshow': [1609.0], 
                    'cobram': [1609.0, 1670.0, 2150.0, 2170.0, 2618.0, 2678.0, 3158.0, 3178.0], 
                    'collie': [1750.0, 2020.0, 2050.0, 2400.0, 2670.0, 2700.0], 
                    'coolamon': [1760.0, 2200.0, 2210.0, 2808.0, 3258.0], 
                    'cootamundra': [1730.0, 2140.0, 2530.0], 
                    'cowra': [1700.0, 1707.0, 2100.0, 2110.0, 2160.0, 2510.0, 2520.0, 2950.0, 2970.0], 
                    'cranbourne': [1155.0, 1609.0, 1630.0, 2080.0, 2105.0, 2110.0, 2115.0, 2555.0, 2575.0, 2585.0], 
                    'cunderdin': [1730.0, 2130.0, 2145.0, 2500.0, 2530.0, 2540.0], 
                    'deagon (grass)': [1950.0], 
                    'devonport': [1900.0, 1910.0, 1930.0, 2286.0, 2297.0, 2645.0, 2665.0, 3020.0], 
                    'donald': [1950.0, 2400.0], 
                    'dubbo': [1315.0, 1320.0, 1720.0, 2100.0, 2120.0, 2520.0, 2525.0], 
                    'echuca': [1755.0, 2130.0, 2160.0, 2530.0, 2560.0], 
                    'elmore': [1780.0, 1800.0, 2180.0, 2200.0, 2600.0], 
                    'eugowra': [1700.0, 2100.0, 2500.0], 
                    'fairfield': [1313.0, 1713.0, 2113.0, 2133.0, 2513.0, 2533.0], 
                    'forbes': [1680.0, 2060.0, 2470.0], 
                    'gatton': [1620.0, 1850.0], 
                    'gawler': [1360.0, 1580.0, 1590.0, 1609.0, 1750.0, 1780.0, 2050.0, 2070.0, 
                               2170.0, 2540.0, 2550.0, 2570.0, 2580.0], 
                    'geelong': [1140.0, 1609.0, 2100.0, 2570.0], 
                    'geraldton': [1609.0, 1700.0, 1735.0, 1750.0, 2100.0, 2135.0, 2150.0, 2500.0, 2535.0, 2550.0], 
                    'globe derby': [1385.0, 1800.0, 2070.0, 2170.0, 2230.0, 2645.0, 18000.0], 
                    'gloucester park': [900.0, 1700.0, 1730.0, 1740.0, 1742.0, 2096.0, 2100.0, 
                                        2102.0, 2130.0, 2140.0, 2143.0, 2500.0, 2503.0, 2506.0, 
                                        2530.0, 2536.0, 2544.0, 2548.0, 2900.0, 2902.0, 2903.0, 
                                        2906.0, 2907.0, 2936.0, 3309.0], 
                    'gold coast': [166.0, 182.0, 1100.0, 1609.0, 2100.0, 2609.0, 3100.0], 
                    'goulburn': [1710.0, 1720.0, 2240.0, 2760.0], 
                    'griffith': [1750.0, 2150.0], 
                    'gunbower': [1390.0, 2030.0, 2630.0], 
                    'gympie': [1500.0, 1609.0, 1850.0, 1950.0], 
                    'gympie show': [1609.0], 
                    'hamilton': [1660.0, 1795.0, 2160.0, 2180.0, 2210.0, 2580.0, 2660.0], 
                    'harold park': [1760.0, 2160.0, 2565.0, 2965.0, 3370.0], 
                    'harvey': [1735.0, 1750.0, 2100.0, 2135.0, 2150.0, 2500.0, 2535.0, 2550.0], 
                    'hobart': [1120.0, 1140.0, 1609.0, 1680.0, 2000.0, 2090.0, 2400.0, 2579.0, 3060.0], 
                    'hobart s': [1609.0, 2000.0, 2360.0, 2750.0], 
                    'horsham': [1200.0, 1609.0, 1700.0, 2200.0, 2700.0], 
                    'inverell': [1639.0, 2000.0, 2069.0, 2070.0], 
                    'junee': [1760.0, 2170.0, 2510.0], 
                    'kadina': [2064.0, 2376.0], 
                    'kalgoorlie': [400.0, 1750.0, 2118.0, 2150.0, 2518.0, 2550.0, 2918.0], 
                    'kapunda': [900.0, 980.0, 1400.0, 1800.0, 2200.0, 2220.0, 2600.0, 2610.0, 3430.0], 
                    'kellerb': [1730.0, 2100.0, 2130.0, 2500.0, 2530.0], 
                    'kellerberrin': [400.0, 1700.0, 1710.0, 1730.0, 1740.0, 2100.0, 2116.0, 
                                     2130.0, 2190.0, 2500.0, 2530.0], 
                    'kilcoy': [1600.0, 1800.0, 1820.0, 1850.0], 
                    'kilmore': [1180.0, 1660.0, 1690.0, 2150.0, 2180.0, 2660.0, 2690.0, 3150.0], 
                    'kimba': [1920.0, 2320.0, 2360.0], 
                    'king island': [1609.0, 1613.0, 1680.0, 1690.0, 1735.0, 1750.0, 2000.0, 2079.0, 
                                    2109.0, 2130.0, 2150.0, 2428.0, 2438.0, 3300.0], 
                    'kngaroy': [1609.0], 
                    'launceston': [1609.0, 1680.0, 2200.0, 2698.0, 3218.0], 
                    'leeton': [1300.0, 1310.0, 1317.0, 1710.0, 1720.0, 1758.0, 2147.0, 2558.0, 2582.0], 
                    'lithgow': [1609.0, 1980.0, 2363.0], 
                    'mackay': [1320.0, 1680.0, 1740.0, 2160.0, 2580.0, 3000.0], 
                    'maitland': [1609.0, 1644.0, 1646.0, 1648.0, 1656.0, 2044.0, 2054.0, 2422.0, 2432.0, 2834.0], 
                    'marburg': [100.0, 1500.0, 1850.0, 2200.0, 2550.0, 2900.0], 
                    'maryborough': [1609.0, 1690.0, 1912.0, 2190.0, 2690.0], 
                    'maryborough (qld)': [1912.0], 
                    'melton': [1200.0, 1720.0, 2240.0, 2760.0, 3280.0], 
                    'menangle': [1000.0, 1609.0, 2300.0, 2400.0, 3009.0], 
                    'mildura': [1380.0, 1790.0, 2190.0, 2600.0], 
                    'moonee valley': [1130.0, 1135.0, 1609.0, 2090.0, 2100.0, 2570.0, 2575.0, 3050.0, 3065.0], 
                    'mooroopna': [1750.0, 2150.0, 2550.0], 
                    'mount gambier': [1385.0, 1780.0, 1790.0, 2170.0, 2190.0, 2580.0, 2590.0], 
                    'muswellbrook': [1770.0, 2170.0], 
                    'nanango show': [1609.0, 1620.0, 1700.0], 
                    'narrabri': [1760.0, 2160.0, 2560.0], 
                    'narrogin': [400.0, 1795.0, 1823.0, 1828.0, 1842.0, 1862.0, 2100.0, 2223.0, 2230.0, 
                                 2242.0, 2262.0, 2620.0, 2636.0, 2648.0, 2662.0, 2682.0, 2685.0], 
                    'new norfolk': [1700.0, 1730.0, 1750.0, 1780.0, 2100.0, 2125.0, 2150.0, 2160.0, 
                                    2500.0, 2535.0, 2550.0, 2580.0, 2900.0, 2950.0], 
                    'newcastle': [1100.0, 1609.0, 2030.0, 2550.0, 2970.0], 
                    'northam': [400.0, 1750.0, 1780.0, 1790.0, 1795.0, 2150.0, 2170.0, 2180.0, 2190.0, 
                                2205.0, 2295.0, 2503.0, 2560.0, 2590.0, 2605.0, 2620.0, 2970.0], 
                    'nowra': [1750.0, 2150.0], 
                    'nyah': [1765.0, 2170.0, 2570.0], 
                    'orange': [1527.0, 1880.0, 2240.0], 
                    'orange racecourse': [1609.0], 
                    'ouyen': [1650.0, 1654.0, 2035.0, 2040.0, 2415.0, 2423.0, 2800.0, 2809.0], 
                    'parkes': [1660.0, 2040.0, 2430.0, 2807.0], 
                    'peak hill': [1800.0, 2250.0], 
                    'penrith': [1295.0, 1720.0, 1740.0, 2095.0, 2120.0, 2125.0, 2135.0, 2140.0, 2525.0], 
                    'pinjarra': [400.0, 1170.0, 1177.0, 1609.0, 1631.0, 1670.0, 1684.0, 2100.0, 2116.0, 
                                 2118.0, 2130.0, 2150.0, 2170.0, 2184.0, 2185.0, 2609.0, 2631.0, 2636.0, 2670.0, 2692.0], 
                    'port pirie': [1100.0, 1609.0, 1613.0, 2050.0, 2054.0, 2059.0, 2530.0, 2534.0, 2550.0], 
                    'redcliffe': [947.0, 1207.0, 1660.0, 1780.0, 2040.0, 2280.0, 2613.0], 
                    'rna showgrounds': [1609.0], 
                    'robinvale': [1880.0, 2250.0, 2598.0], 
                    'rockhampton': [1734.0, 2123.0, 2134.0, 2534.0], 
                    'rocklea': [1698.0, 1969.0, 2244.0, 2510.0, 2513.0], 
                    'scottsdale': [1955.0, 2692.0], 
                    'shepparton': [1190.0, 1609.0, 1670.0, 1690.0, 1890.0, 2170.0, 2190.0, 2280.0,
                                2310.0, 2670.0, 2690.0, 2710.0, 2740.0], 
                    'south australia': [1100.0, 1609.0, 1780.0, 2050.0, 2190.0, 2530.0, 2580.0, 2590.0], 
                    'spreyton': [1700.0, 1865.0], 
                    'st arnaud': [1740.0, 2110.0, 2120.0, 2140.0, 2150.0, 2510.0, 2520.0], 
                    'st marys': [1650.0, 1800.0, 1810.0, 2010.0, 2400.0, 2685.0], 
                    'stawell': [1780.0, 1785.0, 2175.0, 2180.0, 2590.0, 2600.0], 
                    'strathalbyn': [1305.0, 1710.0, 2110.0, 2510.0], 
                    'swan hill': [1200.0, 1609.0, 1750.0, 2240.0, 2790.0], 
                    'tamworth': [1230.0, 1609.0, 1900.0, 1980.0, 2360.0, 2730.0, 3110.0], 
                    'temora': [1609.0, 1635.0, 2000.0, 2360.0, 2386.0], 
                    'terang': [1680.0, 2180.0, 2680.0], 
                    'toowoomba': [1660.0, 1800.0, 2060.0], 
                    'toowoomba showground': [1609.0, 2209.0], 
                    'townsville': [1609.0, 1915.0, 2206.0], 
                    'tweed heads': [1644.0, 2073.0], 
                    'victor harbor': [1609.0, 1660.0, 2160.0, 2660.0],
                    'wagga': [1360.0, 1365.0, 1740.0, 1755.0, 2160.0, 2165.0, 2270.0, 2565.0, 2575.0], 
                    'wagin': [400.0, 1776.0, 1780.0, 1795.0, 2165.0, 2180.0, 2195.0, 2530.0, 2540.0, 2550.0, 2590.0], 
                    'wangaratta': [1800.0, 2210.0, 2615.0], 
                    'wanneroo': [400.0, 1700.0, 2100.0, 2400.0, 2470.0, 2500.0], 
                    'warragul': [1785.0, 1790.0, 2205.0, 2210.0, 2620.0, 2627.0], 
                    'warrnambool': [1700.0, 2100.0, 2500.0], 
                    'warwick': [1200.0, 1350.0, 1500.0, 1609.0, 1950.0, 2950.0], 
                    'wayville': [1050.0, 2140.0, 2380.0, 2653.0], 
                    'wedderburn': [1740.0, 1750.0, 2140.0, 2150.0, 2540.0, 2555.0, 2955.0], 
                    'west wyalong': [1700.0, 1740.0, 2100.0, 2140.0, 2500.0, 2540.0], 
                    'whyalla': [1000.0, 1920.0, 2320.0, 2323.0, 2360.0], 
                    'williams': [1936.0, 1939.0, 2272.0, 2277.0, 2666.0, 2667.0], 
                    'wyong': [1710.0, 2180.0], 
                    'yarra valley': [1650.0, 2150.0, 2180.0, 2210.0, 2590.0, 2650.0], 
                    'york': [2080.0, 2410.0, 2750.0], 
                    'young': [1700.0, 1710.0, 1720.0, 1740.0, 1770.0, 2100.0, 2400.0, 2480.0, 2887.0]
        }
        

        # THESE should be DYNAMIC CSV's

        self.horse_ids = pd.read_csv('horse_id_valid_combos.csv',)
        self.horse_ids['horseId'] = self.horse_ids['horseId'].astype(str)

        self.driver_ids = pd.read_csv('driver_id_valid_combos.csv')
        self.driver_ids['driverId'] = self.driver_ids['driverId'].astype(str)

        self.trainer_ids = pd.read_csv('trainer_id_valid_combos.csv')
        self.trainer_ids['trainerId'] = self.trainer_ids['trainerId'].astype(str)
        
        self.owner_ids = pd.read_csv('owner_id_valid_combos.csv')
        self.owner_ids['ownerId'] = self.owner_ids['ownerId'].astype(str)

        self.breeder_ids = pd.read_csv('breeder_id_valid_combos.csv')
        self.breeder_ids['breederId'] = self.breeder_ids['breederId'].astype(str)
        
        self.dam_ids = pd.read_csv('dam_id_valid_combos.csv')
        self.dam_ids['damId'] = self.dam_ids['damId'].astype(str)

        self.sire_ids = pd.read_csv('sire_id_valid_combos.csv')
        self.sire_ids['sireId'] = self.sire_ids['sireId'].astype(str)

        self.broodmare_sire_ids = pd.read_csv('broodmareSire_id_valid_combos.csv')
        self.broodmare_sire_ids['broodmareSireId'] = self.broodmare_sire_ids['broodmareSireId'].astype(str)