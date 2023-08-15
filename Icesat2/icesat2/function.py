import geohash


def to_geohash(row):
    return geohash.encode(
        latitude=row['latitude'], longitude=row['longitude'], precision=3
    )


def geohash_lapig(tmp_df):
    tmp_df['geohash'] = tmp_df.apply(to_geohash, axis=1)
    filtered_df = tmp_df[
        tmp_df['geohash'].str.startswith(('d', '6', '7'))
    ].copy()
    return filtered_df


def atl82atl3(name):
    return name.replace('ATL08', 'ATL03')
