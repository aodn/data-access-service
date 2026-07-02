"""Email HTML for freeform polygon selections (the counterpart of form_bbox_divs)."""

from data_access_service.utils.email_templates.email_images import (
    POLYGON_IMG,
)


def form_polygon_divs(polygons):
    """Form the email section for every freeform polygon.

    :param polygons: a list of polygons, each a list of [lon, lat] vertices
    :return: the concatenated HTML, or "" when there are no polygons
    """
    if polygons is None or not polygons:
        return ""

    all_divs = ""
    for vertices in polygons:
        all_divs += __form_polygon_div(vertices)

    return all_divs


def __form_coordinate_row(text):
    """Form a single row of text (one polygon vertex)."""
    return f"""
                                        <tr>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="432">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c">
                                                                <p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">{text}</p>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true">
                                                <div style="height:8px;line-height:8px;">&#8203;</div>
                                            </td>
                                        </tr>"""


def __form_polygon_div(vertices):
    """Form one polygon div: a header plus one row per vertex."""
    # List each vertex as a human-numbered "Point N: (lat, lon)" row.
    # GeoJSON stores [lon, lat]; display latitude-first per geographic convention.
    # Index by position so vertices carrying an elevation (a 3rd value) still work.
    coordinate_rows = ""
    for index, point in enumerate(vertices, start=1):
        lon, lat = point[0], point[1]
        coordinate_rows += __form_coordinate_row(f"Point {index}: ({lat}, {lon})")

    one_polygon_div = f"""
    <!--[if mso | IE]></td></tr></table></td></tr><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:568px;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
            <tbody>
            <tr>
                <td style="border:none;direction:ltr;font-size:0;padding:10px 20px 10px 20px;text-align:center;">
                    <!--[if mso | IE]>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                        <tr>
                            <td style="vertical-align:middle;width:528px;">
                    <![endif]-->
                    <div class="l h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
                            <tbody>
                            <tr>
                                <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
                                    <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
                                        <tr>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="32">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%"> <img alt width="32" style="display:block;width:32px;height:32px;" src="data:image/png;base64,{POLYGON_IMG}"></td>
                                                    </tr>
                                                </table>
                                            </td>
                                            <td style="vertical-align:middle;color:transparent;font-size:0;" width="16"></td>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02">
                                                                <p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Polygon Selection</p>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <tr>
                                <td style="font-size:0;padding:0;word-break:break-word;">
                                    <div style="height:8px;line-height:8px;">&#8202;</div>
                                </td>
                            </tr>
                            <tr>
                                <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0px 48px 0px 48px;word-break:break-word;">
                                    <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">{coordinate_rows}
                                    </table>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                    <!--[if mso | IE]>
                    </td>
                    </tr>
                    </table>
                    <![endif]-->
                </td>
            </tr>
            </tbody>
        </table>
    </div>
    <!--[if mso | IE]></td></tr></table></td></tr><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    """

    return one_polygon_div
