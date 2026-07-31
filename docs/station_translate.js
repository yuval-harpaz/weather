/**
 * station_translate.js
 * Loads ims_stations_translated.csv and provides a function to get the
 * Hebrew name for a given latin station name.
 *
 * Usage:
 *   <script src="station_translate.js"></script>
 *   await loadStationTranslations();
 *   const heb = getHebrewName('TEL YOSEF_20060907'); // => 'תל יוסף'
 */

const translatedUrl = 'https://raw.githubusercontent.com/yuval-harpaz/weather/refs/heads/main/data/ims_stations_translated.csv';

let _hebMap = null;

/**
 * Loads and caches the Hebrew name lookup map.
 * Returns a Promise that resolves when done.
 */
function loadStationTranslations() {
    if (_hebMap) return Promise.resolve(_hebMap);
    return d3.csv(translatedUrl).then(rows => {
        _hebMap = new Map();
        rows.forEach(s => {
            const heb = s.hebrew_name ? s.hebrew_name.trim() : '';
            if (!heb) return;
            if (s.matched_english_name && s.matched_english_name.trim())
                _hebMap.set(s.matched_english_name.trim(), heb);
            if (s.another_name && s.another_name.trim())
                _hebMap.set(s.another_name.trim(), heb);
        });
        return _hebMap;
    });
}

/**
 * Returns the Hebrew name for a latin station name.
 * Strips _1m suffix and trailing long numerals (separated by space or _)
 * before looking up the translation.
 * Falls back to the stripped key if no translation is found.
 *
 * @param {string} latinName - e.g. 'TEL YOSEF_20060907' or 'HAIFA_1m'
 * @returns {string} Hebrew name, or the cleaned latin key if not found
 */
function getHebrewName(latinName) {
    if (!_hebMap) {
        console.warn('station_translate.js: translations not loaded yet');
        return latinName;
    }
    let key = latinName;
    // Strip trailing long numeral separated by space or underscore (e.g. _20060907 or  20060907)
    key = key.replace(/[\s_]\d{5,}$/, '');
    // Strip _1m suffix (may be revealed after stripping numeral, e.g. EN GEDI_1m_20231213)
    if (key.endsWith('_1m')) key = key.slice(0, -3);
    return _hebMap.get(key) || key;
}

/**
 * Builds a sorted list of station option objects from an array of latin station names.
 * Each object has: { value, label, hebName }
 * where label is "Hebrew (LATIN)" and list is sorted by Hebrew name.
 *
 * @param {string[]} stations - array of latin station names
 * @returns {{ value: string, label: string, hebName: string }[]}
 */
function buildSortedStationOptions(stations) {
    const list = stations.map(s => {
        const hebName = getHebrewName(s);
        return { value: s, label: `${hebName} (${s})`, hebName };
    });
    list.sort((a, b) => a.hebName.localeCompare(b.hebName, 'he'));
    return list;
}

