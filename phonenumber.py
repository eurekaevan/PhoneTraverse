Number_segments={
    'china_mobile':[
        134,135,136,137,138,139,147,148,150,151,152,157,158,159,172,178,182,183,184,187,188,195,197,198
    ],
    'china_unicom':[
        130,131,132,145,146,155,156,166,175,176,185,186,196
    ],
    'china_telecom':[
        133,153,149,173,177,180,181,189,190,191,193,199
    ],
    'china_broadnet':[
        192
    ]
}
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
prefixes_path = os.path.join(script_dir, "prefixes_phonenumber.txt")
with open(prefixes_path,"w+",encoding="utf-8") as f:
    for key,value in Number_segments.items():
        for segment in value:
            for i in range(10000):
                prefixes = f"{segment}{i:04d}"
                f.write(prefixes + "\n")
print("DONE!")

