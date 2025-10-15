import time
import asyncio
import pandas as pd
import numpy as np
import datetime
import unicodedata, re
from collections import defaultdict
from static.telegram import send_telegram_message
from data_acquisition.data_cleaning import clean_error_dogs, clean_error_races, clean_datafields_formats, \
        bijective_dogname_and_id, bijective_race_ids, trainer_cleaning, check_order_of_runtime_and_places
from sp_data.betfair_data import merge_new_data_with_betfair, check_recent_mongo_for_bsp_updates
from database_management.mongodb import bulk_data_from_mongodb
from static.static_data import StaticData
from static.static_functions import DataFormatting
from stew_reports.meta_processor import MetaProcessor


class SectionalCleaning:
    def __init__(self, read_database=False):
        pass
    
    # ---------- helpers ----------
    

    def _pre_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        
        rename_cols = {
            'date_master': 'date',
            'distance_travelled': 'additional_distance_travelled'
        }
        df.rename(columns=rename_cols, inplace=True)
        df['additional_distance_travelled'] = df['additional_distance_travelled'].apply(lambda x: str(x).replace('m', '').replace('+', '').replace('-', '') if not pd.isna(x) else None)
        bad_name_endings = ['(nz)', 'fin', 'ir', 'fra', 'ita', 'irl', 'usa']
        for ending in bad_name_endings:
            df['horse_name'] = df['horse_name'].apply(lambda x: str(x).replace(f' {ending}', '') if str(x).endswith(ending) else str(x))
        
        track_updates = {
            'tabcorp park menangle': 'menangle',
            'wagga riverina': 'wagga',
            'globe derby park': 'globe derby',
            'central wheatbelt': 'kellerberrin',
        }
        df['track'] = df['track'].apply(lambda x: str(x).lower())
        df.loc[df['track'].isin(track_updates.keys()), 'track'] = df.loc[df['track'].isin(track_updates.keys()), 'track'].map(track_updates)
        df['track'] = df['track'].apply(lambda x: 'menangle' if 'menangle' in x.lower() else x)
        
        return self.clean_names(df)

    def _post_processing(self, df: pd.DataFrame) -> pd.DataFrame:
        # DROP SOME COLUMNS
        cols_to_remove = ['_match_type', 'state_df', 'date_df', 'track_df', 
                            'race_number', 'tab_number', 'horse_name', 
                            '_merge', 'temp_distance_travelled', 
                            'date_test_master', 'date_test_df',
                            'distance_df', 'source_file', 'rank']
        for col in cols_to_remove:
            if col in df.columns:
                df = df.drop(columns=[col])
        col_name_updates = {
            'track_master': 'track',
            'state_master': 'state',
        }
        df.rename(columns=col_name_updates, inplace=True)

        # Check to see which track date combos dont have any sectional data
        from_date = datetime.datetime.now() - datetime.timedelta(days=3)
        df['date'] = pd.to_datetime(df['date'])
        df_analysis = df[df['date'] <= from_date].copy()
        df_analysis['time_400m'] = pd.to_numeric(df_analysis['time_400m'], errors='coerce')

        df_analysis = df_analysis[(pd.isna(df_analysis['time_400m']))]
        df_analysis.to_csv('missing_sectional_data.csv', index=False)
        df_analysis = df_analysis.groupby(['track', 'date']).size().reset_index(name='count')
        df_analysis = df_analysis[df_analysis['count'] > 20]
        print(df_analysis)
        if not df_analysis.empty:
            df_analysis.to_csv('missing_sectional_data.csv', index=False)
            track_date_combos = df_analysis[['track', 'date']].drop_duplicates()
            msg = f'Found {df_analysis.shape[0]} rows of missing sectional data. ' + '\n'.join(track_date_combos['track'] + ' ' + track_date_combos['date'].astype(str))
            print(msg)
            send_telegram_message(msg)
        # df_analysis.to_csv('missing_sectional_data.csv', index=False)

        
        return df
    
    def clean_names(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df['horse_name'] == 'bold comment lomb', 'horse_name'] = 'bold comment lombo'
        df.loc[df['horse_name'] == 'the merchant banke', 'horse_name'] = 'the merchant banker'
        df.loc[df['horse_name'] == 'pacific b', 'horse_name'] = 'pacific benz'
        df.loc[df['horse_name'] == 'internationaltuffgu', 'horse_name'] = 'internationaltuffguy'
        df.loc[df['horse_name'] == 'somepartysomewhe', 'horse_name'] = 'somepartysomewhere'
        df.loc[df['horse_name'] == 'somepartysomewher', 'horse_name'] = 'somepartysomewhere'
        df.loc[df['horse_name'] == 'somemondosomewh', 'horse_name'] = 'somemondosomewhere'
        df.loc[df['horse_name'] == 'somelovesomewher', 'horse_name'] = 'somelovesomewhere'
        df.loc[df['horse_name'] == 'somemagicsomewhe', 'horse_name'] = 'somemagicsomewhere'
        df.loc[df['horse_name'] == 'courage underwate', 'horse_name'] = 'courage underwater'
        df.loc[df['horse_name'] == 'wiliamstown shado', 'horse_name'] = 'wiliamstown shadow'
        df.loc[df['horse_name'] == 'thats my money hon', 'horse_name'] = 'thats my money honey'
        df.loc[df['horse_name'] == 'go ahead makemyda', 'horse_name'] = 'go ahead makemyday'
        df.loc[df['horse_name'] == 'the stairwaytoheave', 'horse_name'] = 'the stairwaytoheaven'
        df.loc[df['horse_name'] == 'im the debt collecto', 'horse_name'] = 'im the debt collector'
        df.loc[df['horse_name'] == 'our mischievious mis', 'horse_name'] = 'our mischievious miss'
        df.loc[df['horse_name'] == 'thortiheardulaugh', 'horse_name'] = 'thortiheardulaughn'
        df.loc[df['horse_name'] == 'somerosesomewher', 'horse_name'] = 'somerosesomewhere'
        df.loc[df['horse_name'] == 'ihitthegroundrunni', 'horse_name'] = 'ihitthegroundrunning'
        df.loc[df['horse_name'] == 'our midnight mayhe', 'horse_name'] = 'our midnight mayhem'
        df.loc[df['horse_name'] == 'our summer vacatio', 'horse_name'] = 'our summer vacation'
        df.loc[df['horse_name'] == 'our lansdowne roa', 'horse_name'] = 'our lansdowne road'
        df.loc[df['horse_name'] == 'our gold dust woma', 'horse_name'] = 'our gold dust woman'
        df.loc[df['horse_name'] == 'uwantsumcomegets', 'horse_name'] = 'uwantsumcomegetsum'
        df.loc[df['horse_name'] == 'uwantsumcomegetsu', 'horse_name'] = 'uwantsumcomegetsum'
        df.loc[df['horse_name'] == 'illawong armstron', 'horse_name'] = 'illawong armstrong'
        df.loc[df['horse_name'] == 'sheza westburn jewe', 'horse_name'] = 'sheza westburn jewel'
        df.loc[df['horse_name'] == 'guna rocktillyoudr', 'horse_name'] = 'guna rocktillyoudrop'
        df.loc[df['horse_name'] == 'my chachingchachin', 'horse_name'] = 'my chachingchaching'
        df.loc[df['horse_name'] == 'seenohearnospeakn', 'horse_name'] = 'seenohearnospeakno'
        df.loc[df['horse_name'] == 'famousinasmalltow', 'horse_name'] = 'famousinasmalltown'
        df.loc[df['horse_name'] == 'begoodorbegoodati', 'horse_name'] = 'begoodorbegoodatit'
        df.loc[df['horse_name'] == 'sombronskisomwhe', 'horse_name'] = 'sombronskisomwhere'
        df.loc[df['horse_name'] == 'sombronskisomwher', 'horse_name'] = 'sombronskisomwhere'
        df.loc[df['horse_name'] == 'whenthegroundsho', 'horse_name'] = 'whenthegroundshook'
        df.loc[df['horse_name'] == 'nothnbutagoodthin', 'horse_name'] = 'nothnbutagoodthing'
        df.loc[df['horse_name'] == 'somebeach n clovell', 'horse_name'] = 'somebeach n clovelly'
        df.loc[df['horse_name'] == 'savesomtimetodrea', 'horse_name'] = 'savesomtimetodream'
        df.loc[df['horse_name'] == 'our chain of comma', 'horse_name'] = 'our chain of command'

        df.loc[df['horse_name'] == 'fullon boundary ro', 'horse_name'] = 'fullon boundary row'
        df.loc[df['horse_name'] == 'luisanabelle midfre', 'horse_name'] = 'luisanabelle midfrew'
        df.loc[df['horse_name'] == 'meadowmae hanove', 'horse_name'] = 'meadowmae hanover'
        df.loc[df['horse_name'] == 'tulhurst daydreame', 'horse_name'] = 'tulhurst daydreamer'
        df.loc[df['horse_name'] == 'what a curtainraise', 'horse_name'] = 'what a curtainraiser'
        df.loc[df['horse_name'] == 'wegottarocketmach', 'horse_name'] = 'wegottarocketmachine'
        df.loc[df['horse_name'] == 'here for the goodti', 'horse_name'] = 'here for the goodtimes'
        df.loc[df['horse_name'] == 'bella sa', 'horse_name'] = 'bella sainz'
        df.loc[df['horse_name'] == 'our wall street wol', 'horse_name'] = 'our wall street wolf'
        df.loc[df['horse_name'] == 'howharddoyawann', 'horse_name'] = 'howharddoyawannago'
        df.loc[df['horse_name'] == 'spirit of anzac', 'horse_name'] = 'spirit and soul'
        df.loc[df['horse_name'] == 'spirit and soul nz (late spirit of anzac nz)', 'horse_name'] = 'spirit and soul'

        df.loc[df['horse_name'] == 'jyenamite', 'horse_name'] = 'the mayor'
        df.loc[df['horse_name'] == 'miss what usain', 'horse_name'] = 'miss whatin'
        df.loc[df['horse_name'] == 'young miss mach', 'horse_name'] = 'young lady mach'
        df.loc[df['horse_name'] == 'crackamoa', 'horse_name'] = 'cracka star'
        df.loc[df['horse_name'] == 'love me again nz (late: love me two nz)', 'horse_name'] = 'love me again'
        df.loc[df['horse_name'] == 'love me two', 'horse_name'] = 'love me again'

        return df
    
    def _clean_name(self, x: str) -> str:
        if x is None:
            return ""
        s = str(x)
        s = unicodedata.normalize("NFKD", s).lower().strip()
        s = re.sub(r"\s+", " ", s)          # collapse whitespace to single spaces
        s = re.sub(r"[^a-z0-9 ]", "", s)    # keep letters/digits/space
        return s

    def _nospace(self, s: str) -> str:
        return s.replace(" ", "")

    def _prefix_ok_adaptive(self, a: str, b: str, *, min_common_len: int,
                            max_tail_pct: float, max_next_word_len: int) -> bool:
        # choose (shorter, longer)
        if len(a) <= len(b):
            s, t = a, b
        else:
            s, t = b, a

        if len(s) < min_common_len:
            return False
        if not t.startswith(s):
            return False

        tail_len   = len(t) - len(s)
        tail_limit = int(len(s) * max_tail_pct)

        if tail_len <= tail_limit:
            return True

        # allow a single appended word up to max_next_word_len
        if len(t) > len(s) and (len(a) <= len(b) and b or a)[len(s):len(s)+1] == " ":
            # find length of next token after the space
            next_space = t.find(" ", len(s) + 1)
            word_tail_len = (len(t) - (len(s) + 1)) if next_space == -1 else (next_space - (len(s) + 1))
            return word_tail_len <= max_next_word_len

        return False

    def _score_tuple(self, shorter_len: int, longer_len: int):
        # lower is better
        return (abs(longer_len - shorter_len), -shorter_len, -longer_len)

    # ---------- main ----------
    def staged_merge_with_aliases(
        self,
        df_master: pd.DataFrame,
        df: pd.DataFrame,
        *,
        date_col: str = "date",
        left_name_col: str = "horseName",
        right_name_col: str = "horse_name",
        extra_exact_keys: list[str] | None = None,   # Stage A only
        min_common_len: int = 4,
        prefix_len: int = 6,             # prune key for one rescue source
        max_tail_pct: float = 0.30,      # % tail allowed (of shorter)
        max_next_word_len: int = 12,     # appended-word allowance
        add_aliases: bool = True,        # keep many→one alias rows in addition to strict 1→1
        collapse_aliases: bool = False,  # keep one row per (left/date)
        nospace_in_rescue: bool = True,  # NEW: include nospace per-date matching in rescue union
        final_strip_spaces: bool = True, # backstop alias pass using nospace
        debug: bool = False,
    ) -> pd.DataFrame:
        """
        1) Strict 1→1:
        - Stage A: exact on (date, normalized name [, extras])
        - Rescue UNION: B (date+firstN), C (date+first2), D (per-date),  NS (per-date on nospace)
            using adaptive prefix rule (percent tail or appended word).
        - Global selection: mutual-best → degree-1 → greedy.
        2) Optional alias fill (many→one) on spaced names, then optional final nospace alias backstop.
        """
        extra_exact_keys = extra_exact_keys or []

        # Normalize & prep
        L = df_master.copy()
        R = self._pre_processing(df).copy()
        L["_name"] = L[left_name_col].map(self._clean_name)
        R["_name"] = R[right_name_col].map(self._clean_name)
        L["_lidx"] = L.index
        R["_ridx"] = R.index

        if date_col in L: L[date_col] = pd.to_datetime(L[date_col], errors="coerce")
        if date_col in R: R[date_col] = pd.to_datetime(R[date_col], errors="coerce")

        common_extra = [k for k in extra_exact_keys if (k in L.columns and k in R.columns)]

        # ---- Stage A: exact (locked-in) ----
        exact = pd.merge(
            L[[date_col, "_name", *common_extra, "_lidx"]],
            R[[date_col, "_name", *common_extra, "_ridx"]],
            how="inner",
            on=[date_col, "_name", *common_extra],
        )[["_lidx", "_ridx"]]
        exact["_match_type"] = "exact"
        if debug: print(f"[A] exact: {len(exact)}")

        usedL = set(exact["_lidx"])
        usedR = set(exact["_ridx"])

        base_L = L.loc[~L["_lidx"].isin(usedL)].copy()
        base_R = R.loc[~R["_ridx"].isin(usedR)].copy()

        # ---- Build UNION of rescue candidate edges (li, ri, score) ----
        edges = []

        def _ok(ln, rn):
            return self._prefix_ok_adaptive(ln, rn,
                                    min_common_len=min_common_len,
                                    max_tail_pct=max_tail_pct,
                                    max_next_word_len=max_next_word_len)

        def _add(li, ln, ri, rn):
            s_len = min(len(ln), len(rn)); t_len = max(len(ln), len(rn))
            edges.append((li, ri, self._score_tuple(s_len, t_len)))

        # B: date + first N chars
        if not base_L.empty and not base_R.empty:
            Lb = base_L[[date_col, "_name", "_lidx"]].copy(); Lb["_pref"] = Lb["_name"].str[:prefix_len]
            Rb = base_R[[date_col, "_name", "_ridx"]].copy(); Rb["_pref"] = Rb["_name"].str[:prefix_len]
            candB = pd.merge(Lb, Rb, how="inner", on=[date_col, "_pref"], suffixes=("_L","_R"))
            if debug: print(f"[B] cand={len(candB)}")
            for _, row in candB.iterrows():
                li = row.get("_lidx_L", row["_lidx"]); ri = row.get("_ridx_R", row["_ridx"])
                if li in usedL or ri in usedR: continue
                ln, rn = row["_name_L"], row["_name_R"]
                if _ok(ln, rn): _add(li, ln, ri, rn)

        # C: date + first TWO chars
        if not base_L.empty and not base_R.empty:
            Lc = base_L[[date_col, "_name", "_lidx"]].copy(); Lc["_fc2"] = Lc["_name"].str[:2]
            Rc = base_R[[date_col, "_name", "_ridx"]].copy(); Rc["_fc2"] = Rc["_name"].str[:2]
            candC = pd.merge(Lc, Rc, how="inner", on=[date_col, "_fc2"], suffixes=("_L","_R"))
            if debug: print(f"[C] cand={len(candC)}")
            for _, row in candC.iterrows():
                li = row.get("_lidx_L", row["_lidx"]); ri = row.get("_ridx_R", row["_ridx"])
                if li in usedL or ri in usedR: continue
                ln, rn = row["_name_L"], row["_name_R"]
                if _ok(ln, rn): _add(li, ln, ri, rn)

        # D: per-date all-pairs (spaced names)
        if not base_L.empty and not base_R.empty:
            R_by_date = {dt: grp for dt, grp in base_R.groupby(date_col)}
            rawD = 0
            for dt, gL in base_L.groupby(date_col):
                if pd.isna(dt): continue
                gR = R_by_date.get(dt)
                if gR is None or gR.empty: continue
                for _, lrow in gL.iterrows():
                    ln, li = lrow["_name"], lrow["_lidx"]
                    for _, rrow in gR.iterrows():
                        rn, ri = rrow["_name"], rrow["_ridx"]
                        if li in usedL or ri in usedR: continue
                        if _ok(ln, rn):
                            _add(li, ln, ri, rn)
                        rawD += 1
            if debug: print(f"[D] raw edges={rawD}")

        # NS: per-date all-pairs on space-stripped names (NEW)
        if nospace_in_rescue and not base_L.empty and not base_R.empty:
            Lns = base_L.assign(_name_ns=base_L["_name"].map(self._nospace))
            Rns = base_R.assign(_name_ns=base_R["_name"].map(self._nospace))
            Rns_by_date = {dt: grp for dt, grp in Rns.groupby(date_col)}
            rawNS = 0
            for dt, gL in Lns.groupby(date_col):
                if pd.isna(dt): continue
                gR = Rns_by_date.get(dt)
                if gR is None or gR.empty: continue
                for _, lrow in gL.iterrows():
                    ln_ns, li = lrow["_name_ns"], lrow["_lidx"]
                    for _, rrow in gR.iterrows():
                        rn_ns, ri = rrow["_name_ns"], rrow["_ridx"]
                        if li in usedL or ri in usedR: continue
                        if _ok(ln_ns, rn_ns):
                            # score using ns-lengths (consistent)
                            s_len = min(len(ln_ns), len(rn_ns)); t_len = max(len(ln_ns), len(rn_ns))
                            edges.append((li, ri, self._score_tuple(s_len, t_len)))
                        rawNS += 1
            if debug: print(f"[NS] nospace raw edges={rawNS}")

        # Dedup edges keeping best score
        if edges:
            best = {}
            for li, ri, sc in edges:
                key = (li, ri)
                if (key not in best) or (sc < best[key]): best[key] = sc
            edges = [(li, ri, sc) for (li, ri), sc in best.items()]
        if debug: print(f"[Union] edges={len(edges)}")

        # ---- Global selection: mutual-best → degree-1 → greedy ----
        chosen = []
        if edges:
            bestR = {}; bestL = {}
            for li, ri, sc in edges:
                if (li not in bestR) or (sc < bestR[li][1]): bestR[li] = (ri, sc)
                if (ri not in bestL) or (sc < bestL[ri][1]): bestL[ri] = (li, sc)
            mutual = []
            for li, (ri, sc) in bestR.items():
                li2, _ = bestL.get(ri, (None, None))
                if li2 == li: mutual.append((li, ri, sc))
            mutual.sort(key=lambda x: x[2])
            usedL = set(exact["_lidx"]); usedR = set(exact["_ridx"])
            for li, ri, sc in mutual:
                if li in usedL or ri in usedR: continue
                chosen.append((li, ri)); usedL.add(li); usedR.add(ri)

            # degree-1
            adjL = defaultdict(list); adjR = defaultdict(list)
            for li, ri, sc in edges:
                adjL[li].append((ri, sc)); adjR[ri].append((li, sc))
            changed = True
            while changed:
                changed = False
                for li, nbrs in list(adjL.items()):
                    if li in usedL: continue
                    alive = [(ri, sc) for (ri, sc) in nbrs if ri not in usedR]
                    if len(alive) == 1:
                        ri, sc = alive[0]
                        chosen.append((li, ri)); usedL.add(li); usedR.add(ri); changed = True
                for ri, nbrs in list(adjR.items()):
                    if ri in usedR: continue
                    alive = [(li, sc) for (li, sc) in nbrs if li not in usedL]
                    if len(alive) == 1:
                        li, sc = alive[0]
                        chosen.append((li, ri)); usedL.add(li); usedR.add(ri); changed = True

            # greedy
            remaining = sorted([(li, ri, sc) for (li, ri, sc) in edges if li not in usedL and ri not in usedR],
                            key=lambda x: x[2])
            for li, ri, sc in remaining:
                if li in usedL or ri in usedR: continue
                chosen.append((li, ri)); usedL.add(li); usedR.add(ri)

        rescue_pairs = pd.DataFrame(chosen, columns=["_lidx","_ridx"])
        if not rescue_pairs.empty:
            rescue_pairs["_match_type"] = "rescue"

        # ---- Alias fill (many→one) ----
        alias_pairs = pd.DataFrame(columns=["_lidx","_ridx","_match_type"])
        if add_aliases:
            strict_pairs = pd.concat([exact[["_lidx","_ridx"]], rescue_pairs[["_lidx","_ridx"]]], ignore_index=True)
            left_matched = set(strict_pairs["_lidx"]); right_matched = set(strict_pairs["_ridx"])

            R_un = R.loc[~R["_ridx"].isin(right_matched)].copy()
            if not R_un.empty and left_matched:
                L_matched = L.loc[L["_lidx"].isin(left_matched), ["_lidx","_name",date_col]].copy()
                rows = []
                for dt, gR in R_un.groupby(date_col):
                    if pd.isna(dt): continue
                    gL = L_matched.loc[L_matched[date_col] == dt, ["_lidx","_name"]]
                    if gL.empty: continue
                    for _, rrow in gR.iterrows():
                        rn, ri = rrow["_name"], rrow["_ridx"]
                        best_li, best_sc = None, None
                        for _, lrow in gL.iterrows():
                            ln, li = lrow["_name"], lrow["_lidx"]
                            if not _ok(ln, rn): continue
                            sc = self._score_tuple(min(len(ln), len(rn)), max(len(ln), len(rn)))
                            if (best_sc is None) or (sc < best_sc):
                                best_li, best_sc = li, sc
                        if best_li is not None:
                            rows.append((best_li, ri, "alias"))
                if rows:
                    alias_pairs = pd.DataFrame(rows, columns=["_lidx","_ridx","_match_type"])
                    strict_set = set(map(tuple, strict_pairs.to_records(index=False)))
                    alias_pairs = alias_pairs[~alias_pairs.apply(lambda r: (r["_lidx"], r["_ridx"]) in strict_set, axis=1)]

        # ---- Final space-stripped alias backstop ----
        if final_strip_spaces:
            strict_pairs2 = pd.concat([exact[["_lidx","_ridx"]],
                                    rescue_pairs[["_lidx","_ridx"]],
                                    alias_pairs[["_lidx","_ridx"]]], ignore_index=True)
            left_matched2 = set(strict_pairs2["_lidx"]); right_matched2 = set(strict_pairs2["_ridx"])

            L_ns = L.assign(_name_ns=L["_name"].map(self._nospace))
            R_ns = R.assign(_name_ns=R["_name"].map(self._nospace))

            R_un2 = R_ns.loc[~R_ns["_ridx"].isin(right_matched2)].copy()
            if not R_un2.empty and left_matched2:
                L_matched_ns = L_ns.loc[L_ns["_lidx"].isin(left_matched2), ["_lidx","_name_ns",date_col]]
                rows_ns = []
                for dt, gR in R_un2.groupby(date_col):
                    if pd.isna(dt): continue
                    gL = L_matched_ns.loc[L_matched_ns[date_col] == dt, ["_lidx","_name_ns"]]
                    if gL.empty: continue
                    for _, rrow in gR.iterrows():
                        rn_ns, ri = rrow["_name_ns"], rrow["_ridx"]
                        best_li, best_sc = None, None
                        for _, lrow in gL.iterrows():
                            ln_ns, li = lrow["_name_ns"], lrow["_lidx"]
                            if not self._prefix_ok_adaptive(ln_ns, rn_ns,
                                                    min_common_len=min_common_len,
                                                    max_tail_pct=max_tail_pct,
                                                    max_next_word_len=max_next_word_len):
                                continue
                            sc = self._score_tuple(min(len(ln_ns), len(rn_ns)), max(len(ln_ns), len(rn_ns)))
                            if (best_sc is None) or (sc < best_sc):
                                best_li, best_sc = li, sc
                        if best_li is not None:
                            rows_ns.append((best_li, ri, "alias"))
                if rows_ns:
                    alias_ns = pd.DataFrame(rows_ns, columns=["_lidx","_ridx","_match_type"])
                    strict_set2 = set(map(tuple, strict_pairs2.to_records(index=False)))
                    alias_ns = alias_ns[~alias_ns.apply(lambda r: (r["_lidx"], r["_ridx"]) in strict_set2, axis=1)]
                    alias_pairs = pd.concat([alias_pairs, alias_ns], ignore_index=True)

        # ---- Combine pairs ----
        pairs = pd.concat([exact, rescue_pairs, alias_pairs], ignore_index=True)

        # Attach data (suffixes → *_master / *_df)
        left_with_ptr = pd.merge(L.drop(columns=["_name"]), pairs, how="left", on="_lidx")
        merged = pd.merge(
            left_with_ptr,
            R.drop(columns=["_name"]),
            how="left",
            on="_ridx",
            suffixes=("_master","_df")
        )

        merged["_merge"] = merged["_ridx"].notna().map({True: "both", False: "left_only"})
        if "_match_type" not in merged.columns:
            merged["_match_type"] = pd.NA

        # Efficient right_only append
        matched_r_all = set(pairs["_ridx"])
        right_only = R.loc[~R["_ridx"].isin(matched_r_all)].copy()
        if not right_only.empty:
            colliders = set(left_with_ptr.columns) & set(right_only.columns)
            rename_map = {c: (f"{c}_df" if (f"{c}_df" in merged.columns) else c) for c in colliders}
            rename_map["_ridx"] = "_ridx"
            ro = right_only.rename(columns=rename_map)
            ro["_merge"] = "right_only"
            ro["_match_type"] = pd.NA
            # align once
            missing_cols = [c for c in merged.columns if c not in ro.columns]
            if missing_cols:
                ro = pd.concat([ro, pd.DataFrame({c: pd.NA for c in missing_cols}, index=ro.index)], axis=1)
            ro = ro[merged.columns]
            merged = pd.concat([merged, ro], ignore_index=True, sort=False)

        # Optional: collapse to one row per (left/date)
        if collapse_aliases:
            def _pick_best(group: pd.DataFrame):
                exact_rows = group[group["_match_type"] == "exact"]
                if not exact_rows.empty:
                    return exact_rows.iloc[[0]]
                rn = group.get(right_name_col, group.get(f"{right_name_col}_df"))
                if rn is not None:
                    idx = rn.str.len().fillna(0).idxmax()
                    return group.loc[[idx]]
                return group.iloc[[0]]

            key_cols = []
            key_cols.append(date_col if f"{date_col}_master" not in merged.columns else f"{date_col}_master")
            key_cols.append(left_name_col if f"{left_name_col}_master" not in merged.columns else f"{left_name_col}_master")
            merged = (merged.groupby(key_cols, dropna=False, group_keys=False)
                            .apply(_pick_best)
                            .reset_index(drop=True))

        # Cleanup helper ids
        merged = merged.drop(columns=[c for c in ["_lidx","_ridx"] if c in merged.columns])

        if debug:
            print(f"[Summary] both={(merged['_merge']=='both').sum()} | "
                f"left_only={(merged['_merge']=='left_only').sum()} | "
                f"right_only={(merged['_merge']=='right_only').sum()} | "
                f"aliases={(merged['_match_type']=='alias').sum()}")

        # Telegram notification for sanity checking non-matching data. Ie right merge only 
        # right_only_df = merged[merged['_merge']=='right_only']
        # if not right_only_df.empty:
        #     send_telegram_message(f'Found {right_only_df.shape[0]} rows of non-matching data. Ie right merge only')
        #     right_only_df.to_csv('right_only_df.csv', index=False)
        #     df_master.to_csv('df_master_during_sectional_cleaning.csv', index=False)

        return self._post_processing(merged)

