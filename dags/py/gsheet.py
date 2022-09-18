from utils.df_handle import *

def update_d_tttt():
    print(datetime.now())
    CANTHO_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQ14feQXGyDq2fz-bXWUc0M3JBjMqVaODz-oynx8polZ2PVFpUzywva_SyQHUl9SOdjAGluFC8hcBTd/pub?gid=0&single=true&output=csv'
    DANANG_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRtCp6WUTZZqdB58qwj-U4D96lX_bLYaycTLfs9KzHB9AjEdca5Nze9_RCTckpb3oHajFNZQU22hMcl/pub?gid=0&single=true&output=csv'
    DONGNAI_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTrJr0oT1ecunKAJax724z6Bbp00A3oFDXhmP8IjY-d22AMEv41GOR9wPOtqCHo3CUaguBlN2BdKt20/pub?gid=0&single=true&output=csv'
    HCM_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vS1kl9m9UjXsOKFexUxdqh362B764_400AAkcmwnVKp_0Vmzh-d9fc3aS-GgKvTOpsIQ5_MSJYBoba_/pub?gid=0&single=true&output=csv'
    HN_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSzDM0zpkG3s1DdLPNm6s9PH8P1eLeGt-i3KNoajBcErt-wJEfk7xQ83NDJsW0OwqZi_QBc_Vym2LEl/pub?gid=0&single=true&output=csv'
    KHANHHOA_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSpD3yYDNhV9iuHdBUN98s9nQef1ISxlP4uDw5-CKF8vx2ZzYIyb8ZRSwDEHIOLRyV-DXs85-lwOkTd/pub?gid=0&single=true&output=csv'
    NGHEAN_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQWLU2Ay0f2AqozDuM1uUaqLtqCczzju0s26k6NQVCW5vAvjFy5c9ZlouMuBwdL07SnrQFURxcjjV2g/pub?gid=0&single=true&output=csv'
    QUOCQUANG_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSV9qglEc2gmvOFzVHIxpvA9f5x3kvBirSW7lHjeMRE-bfW8nuWChi9wLwTJGtF5xCp4hEHp91xPCtl/pub?gid=0&single=true&output=csv'
    TANKHA_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQPttJ34dHuSIJ9kK6Q2PNV3NzZJzoY0RxcmxOEU_0HGh7Wx1ZFwiwfQEvT1A4H7yPwrkBeZU02a1rW/pub?gid=0&single=true&output=csv'

    # TANKHA = pd.read_csv(TANKHA_URL)

    CANTHO = pd.read_csv(CANTHO_URL)
    DANANG = pd.read_csv(DANANG_URL)
    DONGNAI = pd.read_csv(DONGNAI_URL)
    HCM = pd.read_csv(HCM_URL)
    HN = pd.read_csv(HN_URL)
    KHANHHOA = pd.read_csv(KHANHHOA_URL)
    NGHEAN = pd.read_csv(NGHEAN_URL)
    QUOCQUANG = pd.read_csv(QUOCQUANG_URL)
    TANKHA = pd.read_csv(TANKHA_URL)

    df_all = union_all([CANTHO,DANANG,DONGNAI,HCM,HN,KHANHHOA,NGHEAN,QUOCQUANG,TANKHA])

    # df_all.to_csv('abc.csv')

    df_all.columns = cleancols(df_all)

    df_all.columns = lower_col(df_all)

    df_all.columns

    # df_all1 = df_filter(df_all, codekh='000146').copy()

    # df_all1

    # df_all1['t4w4']==np.NAN

    # df_all1['thongtinthanhtoangannhat'] = \
    #     np.where(df_all1['t4w4'].isna(), df_all1['t4w3'], "OK")

    # df_all1['thongtinthanhtoangannhat']

    df_all['thongtinthanhtoangannhat'] = \
        np.where(df_all['t4w4'].isna(), df_all['t4w3'], \
            np.where(df_all['t4w3'].isna(), df_all['t4w2'], df_all['t4w1']
        ))

    # vc(df_all, 'thongtinthanhtoangannhat')

    ctr1 = df_all['giaiphap'].notna()
    ctr2 = df_all['thongtinthanhtoangannhat'].notna()

    FINAL = df_all[ctr1 | ctr2][['codekh','tenkh','giaiphap','thongtinthanhtoangannhat']].copy()

    del(df_all)

    col_name = ['makhcu', 'tenkh', 'giaiphap', 'thongtinthanhtoangannhat']

    FINAL.columns = col_name

    FINAL = dropdup(FINAL, 1, subset=['makhcu'])

    commit_psql("truncate table d_tttt cascade;")

    execute_values_insert(FINAL, "d_tttt")