import logging
import pandas
from datetime import datetime
from data_access_service.models.subset_request import NON_SPECIFIED
from data_access_service.utils.email_templates.subsetting_geometry import (
    split_bboxes_and_polygons,
)

logger = logging.getLogger(__name__)
from data_access_service.utils.email_templates.form_bbox_divs import form_bbox_divs
from data_access_service.utils.email_templates.form_polygon_divs import (
    form_polygon_divs,
)
from data_access_service.utils.email_templates.email_images import (
    TIME_RANGE_IMG,
)


def _to_display_date(value):
    """Format an ISO date (YYYY-MM-DD) as the Australian dd/mm/yyyy.

    Any value that is not a plain ISO date is returned unchanged.
    """
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return value


def form_subsetting_divs(start_date, end_date, multi_polygon):
    """Form subsetting section with time range, bounding boxes and polygons"""

    # Hide if dates are NON_SPECIFIED or match default values (1970-01-01 to today)
    has_dates = False
    if start_date and end_date:
        start_str = str(start_date).lower()
        end_str = str(end_date).lower()

        # Check if not NON_SPECIFIED and not default values
        is_non_specified = start_str == NON_SPECIFIED or end_str == NON_SPECIFIED
        is_default = (
            start_str == "1970-01-01"
            and end_str == pandas.Timestamp.today().strftime("%Y-%m-%d").lower()
        )

        has_dates = not is_non_specified and not is_default

    # Split the spatial selection into bounding boxes (<=4 vertices) and freeform polygons (>4).
    # Never let a malformed geometry break the whole email - omit the spatial section instead.
    try:
        bboxes, polygons = split_bboxes_and_polygons(multi_polygon)
    except Exception:
        logger.warning(
            "Could not parse multi_polygon for email; omitting spatial section",
            exc_info=True,
        )
        bboxes, polygons = [], []
    has_bboxes = len(bboxes) > 0
    has_polygons = len(polygons) > 0

    # If no subsetting data at all, return empty string
    if not has_dates and not has_bboxes and not has_polygons:
        return ""

    # Form spatial divs if they exist
    bbox_divs = form_bbox_divs(bboxes) if has_bboxes else ""
    polygon_divs = form_polygon_divs(polygons) if has_polygons else ""

    # Form time range section if dates exist (Australian dd/mm/yyyy display format)
    time_range_section = ""
    if has_dates:
        display_start = _to_display_date(start_date)
        display_end = _to_display_date(end_date)
        time_range_section = f"""
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
                                                        <td align="left" width="100%"> <img alt width="32" style="display:block;width:32px;height:32px;" src="data:image/png;base64,{TIME_RANGE_IMG}"></td>
                                                    </tr>
                                                </table>
                                            </td>
                                            <td style="vertical-align:middle;color:transparent;font-size:0;" width="16"></td>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02">
                                                                <p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Time Range</p>
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
                                    <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
                                        <tr>
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="432">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02">
                                                                <p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">{display_start} - {display_end}</p>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
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
    <!--[if mso | IE]></td></tr></table></td></tr><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->"""

    # Build complete subsetting section
    return f"""
    <!--[if mso | IE]><tr><td width="600px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:568px;" width="568"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:568px;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
            <tbody>
            <tr>
                <td style="border:none;direction:ltr;font-size:0;padding:16px 20px 4px 20px;text-align:center;">
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
                                            <td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto">
                                                <table border="0" cellpadding="0" cellspacing="0" width="100%">
                                                    <tr>
                                                        <td align="left" width="100%">
                                                            <div style="font-family: 'Open Sans', 'Arial', sans-serif; font-size: 17px; font-weight: 500; line-height: 141%; text-align: left; color: #090c02">
                                                                <p style="Margin:0;mso-line-height-alt:24px;font-size:17px;line-height:141%;">Subsetting for this collection</p>
                                                            </div>
                                                        </td>
                                                    </tr>
                                                </table>
                                            </td>
                                        </tr>
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
    <div style="margin:0px auto;max-width:568px;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
            <tbody>
            <tr>
                <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                    <!--[if mso | IE]>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                        <tr>
                            <td style="vertical-align:top;width:568px;">
                    <![endif]-->
                    <div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                            <tbody>
                            <tr>
                                <td style="vertical-align:top;padding:0;">
                                    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                        <tbody>
                                        <tr>
                                            <td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
                                                <div style="height:0;line-height:0;">&#8202;</div>
                                            </td>
                                        </tr>
                                        </tbody>
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
    {bbox_divs}
    {polygon_divs}
    {time_range_section}
    <div style="margin:0px auto;max-width:568px;">
        <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
            <tbody>
            <tr>
                <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
                    <!--[if mso | IE]>
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0">
                        <tr>
                            <td style="vertical-align:top;width:568px;">
                    <![endif]-->
                    <div class="o h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
                        <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                            <tbody>
                            <tr>
                                <td style="vertical-align:top;padding:0;">
                                    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
                                        <tbody>
                                        <tr>
                                            <td style="font-size:0;padding:0;word-break:break-word;" aria-hidden="true">
                                                <div style="height:0;line-height:0;">&#8202;</div>
                                            </td>
                                        </tr>
                                        </tbody>
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
    <!--[if mso | IE]></td></tr></table></td></tr><![endif]-->"""
