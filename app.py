import datetime as dt
import pandas as pd
import streamlit as st

from kundli_services import (
    ProkeralaClient,
    geocode_place,
    infer_timezone,
    local_birth_to_iso_with_offset,
)

st.set_page_config(page_title="Kundli Generator", page_icon="🔯", layout="centered")
st.title("🔯 Kundli Generator")
st.caption("Prokerala Astrology API • Chart + Divisional Planet Position + Kundli + Dasha Periods (Lahiri default)")

# ---------------- Helpers ----------------
def flatten_divisional_positions(resp_json):
    root = resp_json.get("data", resp_json)
    positions = root.get("divisional_positions") or root.get("positions") or []
    rows = []
    for house in positions:
        house_info = house.get("house", {}) or {}
        rasi_info = house.get("rasi", {}) or {}
        rasi_lord = (rasi_info.get("lord") or {})
        house_no = house_info.get("number")
        house_name = house_info.get("name")
        rasi_name = rasi_info.get("name")
        rasi_lord_name = rasi_lord.get("vedic_name") or rasi_lord.get("name")

        planet_positions = house.get("planet_positions") or []
        for p in planet_positions:
            planet = p.get("planet") or {}
            nak = p.get("nakshatra") or {}

            planet_name = planet.get("name") or planet.get("vedic_name")
            planet_vedic = planet.get("vedic_name") or ""
            nak_name = nak.get("name")
            nak_lord = (nak.get("lord") or {})
            nak_lord_name = nak_lord.get("vedic_name") or nak_lord.get("name")
            pada = nak.get("pada") or p.get("pada")

            degree = (
                p.get("degree")
                or p.get("longitude")
                or (p.get("position") or {}).get("degree")
            )
            retro = p.get("retro")
            if retro is None:
                retro = p.get("is_retro")
            if retro is None:
                retro = (p.get("motion") == "retrograde")

            rows.append(
                {
                    "House No": house_no,
                    "House": house_name,
                    "Rasi": rasi_name,
                    "Rasi Lord": rasi_lord_name,
                    "Planet": planet_name,
                    "Planet (Vedic)": planet_vedic,
                    "Nakshatra": nak_name,
                    "Nakshatra Lord": nak_lord_name,
                    "Pada": pada,
                    "Degree": degree,
                    "Retro": bool(retro) if retro is not None else None,
                }
            )
    try:
        rows.sort(key=lambda r: (r["House No"] if r["House No"] is not None else 99, str(r["Planet"])))
    except Exception:
        pass
    return rows


def flatten_kundli_planets(resp_json):
    root = resp_json.get("data", resp_json)
    candidates = None
    for key in ["planets", "planet_positions", "grahas", "graha_positions", "bodies"]:
        v = root.get(key)
        if isinstance(v, list):
            candidates = v
            break
    if not candidates:
        return []
    rows = []
    for p in candidates:
        planet = p.get("planet") or p
        planet_name = planet.get("name") or planet.get("vedic_name")
        planet_vedic = planet.get("vedic_name") or ""
        sign = (p.get("sign") or p.get("rasi") or p.get("zodiac") or {})
        sign_name = sign.get("name")
        sign_lord = (sign.get("lord") or {})
        sign_lord_name = sign_lord.get("vedic_name") or sign_lord.get("name")
        nak = p.get("nakshatra") or {}
        nak_name = nak.get("name")
        nak_lord = (nak.get("lord") or {})
        nak_lord_name = nak_lord.get("vedic_name") or nak_lord.get("name")
        pada = nak.get("pada") or p.get("pada")
        degree = p.get("degree") or p.get("longitude") or (p.get("position") or {}).get("degree")
        house_no = (p.get("house") or {}).get("number")
        retro = p.get("retro")
        if retro is None:
            retro = p.get("is_retro")
        if retro is None:
            retro = (p.get("motion") == "retrograde")
        rows.append(
            {
                "House No": house_no,
                "Planet": planet_name,
                "Planet (Vedic)": planet_vedic,
                "Rasi": sign_name,
                "Rasi Lord": sign_lord_name,
                "Nakshatra": nak_name,
                "Nakshatra Lord": nak_lord_name,
                "Pada": pada,
                "Degree": degree,
                "Retro": bool(retro) if retro is not None else None,
            }
        )
    try:
        rows.sort(key=lambda r: (r["House No"] if r["House No"] is not None else 99, str(r["Planet"])))
    except Exception:
        pass
    return rows


def flatten_kundli_houses(resp_json):
    root = resp_json.get("data", resp_json)
    for key in ["houses", "bhavas", "house_positions", "bhava_positions"]:
        v = root.get(key)
        if isinstance(v, list):
            houses = v
            break
    else:
        return []
    rows = []
    for h in houses:
        house_info = h.get("house", h) or {}
        number = house_info.get("number") or h.get("number")
        name = house_info.get("name") or h.get("name")
        rasi = (h.get("rasi") or h.get("sign") or {})
        rasi_name = rasi.get("name")
        degree = h.get("degree") or h.get("longitude") or (h.get("cusp") or {}).get("degree")
        rows.append({"House No": number, "House": name, "Rasi": rasi_name, "Degree": degree})
    try:
        rows.sort(key=lambda r: r["House No"] if r["House No"] is not None else 99)
    except Exception:
        pass
    return rows


def extract_kundli_summary(resp_json):
    """
    Summary for /kundli responses that contain nakshatra_details, yogas, etc.
    Returns (summary_df, yogas_df, mangal_text)
    """
    root = resp_json.get("data", resp_json)
    nk = root.get("nakshatra_details") or {}
    nak = nk.get("nakshatra") or {}
    chandra_rasi = nk.get("chandra_rasi") or {}
    soorya_rasi = nk.get("soorya_rasi") or {}
    zodiac = nk.get("zodiac") or {}
    add = nk.get("additional_info") or {}

    summary_row = {
        "Moon Nakshatra": nak.get("name"),
        "Pada": nak.get("pada"),
        "Chandra Rasi": chandra_rasi.get("name"),
        "Soorya Rasi": soorya_rasi.get("name"),
        "Zodiac (Tropical)": zodiac.get("name"),
        "Deity": add.get("deity"),
        "Gana": add.get("ganam"),
        "Symbol": add.get("symbol"),
        "Animal Sign": add.get("animal_sign"),
        "Nadi": add.get("nadi"),
        "Color": add.get("color"),
        "Best Direction": add.get("best_direction"),
        "Syllables": add.get("syllables"),
        "Birth Stone": add.get("birth_stone"),
        "Gender": add.get("gender"),
        "Planet (Nakshatra Lord)": add.get("planet"),
        "Enemy Yoni": add.get("enemy_yoni"),
    }
    summary_df = pd.DataFrame([summary_row])

    yogas = root.get("yoga_details") or []
    yoga_rows = [{"Category": y.get("name"), "Description": y.get("description")} for y in yogas]
    yogas_df = pd.DataFrame(yoga_rows) if yoga_rows else pd.DataFrame(columns=["Category", "Description"])

    md = root.get("mangal_dosha") or {}
    if md.get("has_dosha") is True:
        mangal_text = md.get("description") or "Manglik"
    elif md.get("has_dosha") is False:
        mangal_text = md.get("description") or "Not Manglik"
    else:
        mangal_text = "—"

    return summary_df, yogas_df, mangal_text


# ----- Dasha helpers: compute current dasha chain (MD->AD->PD) -----
def _parse_dt(s):
    try:
        return pd.to_datetime(s)
    except Exception:
        return None


def _find_period_covering(periods, now_ts):
    """Return the period dict whose [start,end] contains now_ts."""
    if not isinstance(periods, list):
        return None
    for p in periods:
        s = _parse_dt(p.get("start") or p.get("start_time") or p.get("from"))
        e = _parse_dt(p.get("end") or p.get("end_time") or p.get("to"))
        if s is not None and e is not None and s <= now_ts <= e:
            return p
    return None


def current_dasha_chain(resp_json, now_ts):
    """
    From /dasha-periods JSON, return:
    - current_md, current_ad, current_pd (dicts or None)
    - arrays for all antardasha under current MD and all pratyantardasha under current AD
    - dasha_balance (if present)
    """
    root = resp_json.get("data", resp_json)

    # top-level list of mahadasha periods
    periods = None
    for key in ["periods", "dasha_periods", "dasha", "mahadasha", "maha_dasha", "md"]:
        v = root.get(key)
        if isinstance(v, list):
            periods = v
            break
    if periods is None:
        # sometimes the list is nested inside a named object
        for v in root.values():
            if isinstance(v, dict):
                for k in ["periods", "dasha_periods", "mahadasha"]:
                    if isinstance(v.get(k), list):
                        periods = v.get(k)
                        break
            if periods is not None:
                break

    current_md = _find_period_covering(periods, now_ts) if periods else None
    all_ad = current_md.get("antardasha") if isinstance(current_md, dict) else None
    current_ad = _find_period_covering(all_ad, now_ts) if all_ad else None
    all_pd = current_ad.get("pratyantardasha") if isinstance(current_ad, dict) else None
    current_pd = _find_period_covering(all_pd, now_ts) if all_pd else None

    dasha_balance = root.get("dasha_balance") or {}

    return current_md, current_ad, current_pd, (all_ad or []), (all_pd or []), dasha_balance


def dasha_rows_for_display(md, ad, pd_):
    """Return a small 3-row table for current chain."""
    def row(level, obj):
        if not obj:
            return {"Level": level, "Lord": None, "Start": None, "End": None}
        name = obj.get("name") or (obj.get("planet") or {}).get("vedic_name") or (obj.get("planet") or {}).get("name")
        return {
            "Level": level,
            "Lord": name,
            "Start": obj.get("start") or obj.get("start_time") or obj.get("from"),
            "End": obj.get("end") or obj.get("end_time") or obj.get("to"),
        }

    return [row("Mahadasha", md), row("Antardasha", ad), row("Pratyantardasha", pd_)]

# -------- Persist results across reruns --------
if "results" not in st.session_state:
    st.session_state["results"] = None
if "errors" not in st.session_state:
    st.session_state["errors"] = None

# ---------------- Form (simplified: removed year-length & depth) ----------------
with st.form("kundli_form"):
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Name", value="", placeholder="e.g., Kishor")
        pob = st.text_input("Place of Birth", value="", placeholder="City, State, Country")
        birth_date = st.date_input("Date of Birth", value=dt.date(2001, 12, 29))
    with col2:
        birth_time = st.time_input("Time of Birth", value=dt.time(6, 6))
        chart_type = st.selectbox(
            "Chart Type (for Chart Image)",
            ["rasi", "lagna", "navamsa", "drekkana", "chaturthamsa", "dasamsa", "shodasamsa", "vimsamsa", "bhava"],
            index=0,
        )
        chart_style = st.selectbox("Chart Style", ["north-indian", "south-indian", "east-indian"], index=0)

    div_chart_type = st.selectbox(
        "Divisional Chart Type (for Planet Positions)",
        [
            "lagna", "navamsa", "trimsamsa", "drekkana", "chaturthamsa", "dasamsa", "ashtamsa",
            "dwadasamsa", "shodasamsa", "hora", "akshavedamsa", "shashtyamsa", "panchamsa",
            "khavedamsa", "saptavimsamsa", "chaturvimamsa", "saptamsa", "vimsamsa",
        ],
        index=1,
        help="Lahiri ayanamsa is used by default.",
    )

    submit = st.form_submit_button("Generate Kundli")

# ---------------- On Submit: compute & store ----------------
if submit:
    try:
        if not pob.strip():
            raise ValueError("Please enter a valid Place of Birth.")

        client = ProkeralaClient()

        # Geocode + tz + datetime (local with offset)
        lat, lon, address = geocode_place(pob.strip())
        tz_name = infer_timezone(lat, lon)
        birth_dt_local_iso = local_birth_to_iso_with_offset(birth_date, birth_time, tz_name)

        # Chart
        svg, used_fallback_chart = client.get_chart_svg(
            lat=lat,
            lon=lon,
            birth_dt_local_with_offset_iso=birth_dt_local_iso,
            chart_type=chart_type,
            chart_style=chart_style,
            language="en",
            upagraha_position="middle",
        )

        # Divisional planets
        div_json, used_fallback_div = client.get_divisional_planet_position(
            lat=lat,
            lon=lon,
            birth_dt_local_with_offset_iso=birth_dt_local_iso,
            chart_type=div_chart_type,
            language="en",
        )
        div_rows = flatten_divisional_positions(div_json)
        div_df = pd.DataFrame(div_rows) if div_rows else None

        # Kundli (full horoscope)
        kundli_json, used_fallback_kundli = client.get_kundli(
            lat=lat, lon=lon,
            birth_dt_local_with_offset_iso=birth_dt_local_iso,
            language="en",
        )
        kp_rows = flatten_kundli_planets(kundli_json)
        kh_rows = flatten_kundli_houses(kundli_json)
        kp_df = pd.DataFrame(kp_rows) if kp_rows else None
        kh_df = pd.DataFrame(kh_rows) if kh_rows else None
        ksum_df, kyoga_df, kmangal = extract_kundli_summary(kundli_json)

        # Dasha periods (no UI for depth or year_length; we default to 365.25)
        dasha_json, used_fallback_dasha = client.get_dasha_periods(
            lat=lat, lon=lon,
            birth_dt_local_with_offset_iso=birth_dt_local_iso,
            language="en",
            year_length=1,   # 365.25
        )
        # Compute "current date" in the birth location timezone
        try:
            from zoneinfo import ZoneInfo
            now_local = dt.datetime.now(ZoneInfo(tz_name))
        except Exception:
            import pytz
            now_local = pytz.timezone(tz_name).localize(dt.datetime.now())

        md, ad, pd_, all_ad, all_pd, dasha_balance = current_dasha_chain(dasha_json, pd.to_datetime(now_local))
        current_chain_rows = dasha_rows_for_display(md, ad, pd_)
        current_chain_df = pd.DataFrame(current_chain_rows)
        ad_df = pd.DataFrame([{"Name": x.get("name"), "Start": x.get("start"), "End": x.get("end")} for x in all_ad]) if all_ad else pd.DataFrame()
        pd_df = pd.DataFrame([{"Name": x.get("name"), "Start": x.get("start"), "End": x.get("end")} for x in all_pd]) if all_pd else pd.DataFrame()

        # Store results
        st.session_state["results"] = {
            "address": address, "lat": lat, "lon": lon, "tz_name": tz_name,
            "birth_dt_local_iso": birth_dt_local_iso,

            "chart_svg": svg, "used_fallback_chart": used_fallback_chart,

            "div_json": div_json, "div_df": div_df, "used_fallback_div": used_fallback_div,

            "kundli_json": kundli_json, "kp_df": kp_df, "kh_df": kh_df,
            "kundli_summary_df": ksum_df, "kundli_yogas_df": kyoga_df, "kundli_mangal": kmangal,
            "used_fallback_kundli": used_fallback_kundli,

            "dasha_json": dasha_json, "used_fallback_dasha": used_fallback_dasha,
            "now_local": str(now_local),
            "dasha_chain_df": current_chain_df, "ad_df": ad_df, "pd_df": pd_df,
            "dasha_balance": dasha_balance,
        }
        st.session_state["errors"] = None

    except Exception as e:
        st.session_state["results"] = None
        st.session_state["errors"] = str(e)

# ---------------- Render ----------------
res = st.session_state["results"]
err = st.session_state["errors"]

if err:
    st.error(err)
elif res is None:
    st.info("Fill the form and click **Generate Kundli** to see the chart, divisional planets, kundli summary and current dasha.")
else:
    st.success(f"Location: {res['address']}")
    st.write(f"**Coordinates:** {res['lat']:.6f}, {res['lon']:.6f}")
    st.info(f"Detected Time Zone: `{res['tz_name']}`")
    st.write(f"**Datetime sent (local with offset):** {res['birth_dt_local_iso']}")

    st.divider()
    st.subheader("Chart")
    if res["used_fallback_chart"]:
        st.warning("Sandbox detected for **Chart** → retried with **January 1st** (same year/time/offset).")
    st.components.v1.html(
        f"""<div style="display:flex;justify-content:center;">{res['chart_svg']}</div>""",
        height=520,
        scrolling=False,
    )

    st.divider()
    st.subheader("Divisional Planet Position")
    if res["used_fallback_div"]:
        st.warning("Sandbox detected for **Divisional Planet Position** → retried with **January 1st**.")
    if res["div_df"] is not None and not res["div_df"].empty:
        st.dataframe(res["div_df"], use_container_width=True, height=420)
        st.download_button("Download Divisional CSV",
                           data=res["div_df"].to_csv(index=False).encode("utf-8"),
                           file_name="divisional_planet_positions.csv", mime="text/csv")
    else:
        st.info("Could not flatten divisional rows; showing raw JSON.")
        st.json(res["div_json"])

    st.divider()
    st.subheader("Kundli")
    if res["used_fallback_kundli"]:
        st.warning("Sandbox detected for **Kundli** → retried with **January 1st**.")
    # Tabs: Summary first; Planets/Houses only if present
    tabs = st.tabs(["Summary", "Planets", "Houses", "Raw JSON"])
    with tabs[0]:
        st.markdown(f"**Mangal Dosha:** {res.get('kundli_mangal', '—')}")
        if res.get("kundli_summary_df") is not None:
            st.dataframe(res["kundli_summary_df"], use_container_width=True)
            st.download_button(
                "Download Kundli Summary CSV",
                data=res["kundli_summary_df"].to_csv(index=False).encode("utf-8"),
                file_name="kundli_summary.csv",
                mime="text/csv",
            )
        if res.get("kundli_yogas_df") is not None and not res["kundli_yogas_df"].empty:
            st.markdown("**Yogas**")
            st.dataframe(res["kundli_yogas_df"], use_container_width=True, height=240)
            st.download_button(
                "Download Yogas CSV",
                data=res["kundli_yogas_df"].to_csv(index=False).encode("utf-8"),
                file_name="kundli_yogas.csv",
                mime="text/csv",
            )
    with tabs[1]:
        if res["kp_df"] is not None and not res["kp_df"].empty:
            st.dataframe(res["kp_df"], use_container_width=True, height=420)
            st.download_button("Download Kundli Planets CSV",
                               data=res["kp_df"].to_csv(index=False).encode("utf-8"),
                               file_name="kundli_planets.csv", mime="text/csv")
        else:
            st.info("This /kundli response doesn't include planet positions.")
    with tabs[2]:
        if res["kh_df"] is not None and not res["kh_df"].empty:
            st.dataframe(res["kh_df"], use_container_width=True, height=420)
            st.download_button("Download Kundli Houses CSV",
                               data=res["kh_df"].to_csv(index=False).encode("utf-8"),
                               file_name="kundli_houses.csv", mime="text/csv")
        else:
            st.info("No houses in this /kundli response.")
    with tabs[3]:
        st.json(res["kundli_json"])

    # ---------------- Dasha: Current date chain ----------------
    st.divider()
    st.subheader("Dasha Periods — Current Date Chain")
    if res["used_fallback_dasha"]:
        st.warning("Sandbox detected for **Dasha Periods** → retried with **January 1st**.")

    st.caption(f"Now (local to birth place): {res['now_local']}")
    if res["dasha_chain_df"] is not None and not res["dasha_chain_df"].empty:
        st.dataframe(res["dasha_chain_df"], use_container_width=True)
        st.download_button("Download Current Dasha Chain CSV",
                           data=res["dasha_chain_df"].to_csv(index=False).encode("utf-8"),
                           file_name="dasha_current_chain.csv", mime="text/csv")
    else:
        st.info("Could not determine current dasha chain from the API response.")

    # Show lists under current MD/AD for context
    with st.expander("Antardasha under current Mahadasha"):
        if res["ad_df"] is not None and not res["ad_df"].empty:
            st.dataframe(res["ad_df"], use_container_width=True, height=320)
        else:
            st.write("—")
    with st.expander("Pratyantardasha under current Antardasha"):
        if res["pd_df"] is not None and not res["pd_df"].empty:
            st.dataframe(res["pd_df"], use_container_width=True, height=320)
        else:
            st.write("—")

    # Dasha balance section (lord/duration/description)
    db = res.get("dasha_balance") or {}
    if db:
        st.markdown("**Dasha Balance**")
        lord = db.get("lord") or {}
        balance_df = pd.DataFrame([{
            "Lord": lord.get("vedic_name") or lord.get("name"),
            "Duration (ISO 8601)": db.get("duration"),
            "Description": db.get("description"),
        }])
        st.dataframe(balance_df, use_container_width=True)
