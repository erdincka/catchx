import logging
from nicegui import ui, app, events

from geopy.geocoders import Nominatim

logger = logging.getLogger("map")


def get_city_latlng(city: str):
    geolocator = Nominatim(user_agent="ezdemo")

    location = geolocator.geocode(city)
    return (location.latitude, location.longitude)


def handleClick(e: events.GenericEventArguments):
    logger.debug(e.args)
    # if e.args['layerType'] == 'marker':
    #     print(e.args['layer'])


def meshmap():
    m = ui.leaflet(center=(51.505, -0.090), zoom=6)

    m.clear_layers()
    m.tile_layer(
        url_template=r'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
        options={
            'attribution': '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
            'subdomains': 'abcd',
            'maxZoom': 20
        },
    )

    for cls in app.storage.user['clusters']:
        dc = 'L.icon({iconUrl: "/images/dc.png", iconSize: [32, 32]})'
        edge = 'L.icon({iconUrl: "/images/edge.png", iconSize: [24, 24]})'
        marker = m.marker(latlng=(get_city_latlng(app.storage.user['clusters'][cls])))

        if cls == app.storage.user['cluster']:
            ui.timer(0.1, lambda mr=marker: mr.run_method(':setIcon', dc), once=True)
            m.set_center(marker.latlng)
        else:
            ui.timer(0.1, lambda mr=marker: mr.run_method(':setIcon', edge), once=True)

    return m