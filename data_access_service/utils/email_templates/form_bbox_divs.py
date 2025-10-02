from data_access_service.models.bounding_box import BoundingBox
from data_access_service.utils.email_templates.email_images import (
    BBOX_IMG,
)


def form_bbox_divs(bboxes: [BoundingBox]):
    """Form all bbox divs"""
    if not bboxes:
        return "<p>No bounding boxes specified</p>"

    all_divs = ""
    for i, bbox in enumerate(bboxes):
        div = __form_bbox_div(bbox)

        if div is not None:
            all_divs += div
        else:
            all_divs += "<p>Invalid bounding box</p>"

    return all_divs if all_divs else "<p>No valid bounding boxes</p>"


def __form_bbox_div(bbox: BoundingBox):
    one_bbox_div = f"""
    <!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div
        class="r e y"
        style="
        background: #ffffff;
        background-color: #ffffff;
        margin: 0px auto;
        max-width: 1264px;
        "
    >
        <table
        align="center"
        border="0"
        cellpadding="0"
        cellspacing="0"
        role="presentation"
        style="
            background: #ffffff;
            background-color: #ffffff;
            width: 100%;
        "
        >
        <tbody>
            <tr>
            <td
                style="
                border: none;
                direction: ltr;
                font-size: 0;
                padding: 10px 20px 10px 20px;
                text-align: center;
                "
            >
                <!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:middle;width:600px;"><![endif]-->
                <div
                class="c h"
                style="
                    font-size: 0;
                    text-align: left;
                    direction: ltr;
                    display: inline-block;
                    vertical-align: middle;
                    width: 100%;
                "
                >
                <table
                    border="0"
                    cellpadding="0"
                    cellspacing="0"
                    role="presentation"
                    style="border: none; vertical-align: middle"
                    width="100%"
                >
                    <tbody>
                    <tr>
                        <td
                        align="center"
                        class="tr-0"
                        style="
                            background: transparent;
                            font-size: 0;
                            padding: 0;
                            word-break: break-word;
                        "
                        >
                        <table
                            cellpadding="0"
                            cellspacing="0"
                            width="100%"
                            border="0"
                            style="
                            color: #000000;
                            line-height: normal;
                            table-layout: fixed;
                            width: 100%;
                            border: none;
                            "
                        >
                            <tr>
                            <td
                                align="left"
                                class="u"
                                style="
                                padding: 0;
                                height: auto;
                                word-wrap: break-word;
                                vertical-align: middle;
                                "
                                width="32"
                            >
                                <table
                                border="0"
                                cellpadding="0"
                                cellspacing="0"
                                width="100%"
                                >
                                <tr>
                                    <td align="left" width="100%">
                                    <img
                                        alt
                                        width="32"
                                        style="
                                        display: block;
                                        width: 32px;
                                        height: 32px;
                                        "
                                        src="data:image/png;base64,{BBOX_IMG}"
                                    />
                                    </td>
                                </tr>
                                </table>
                            </td>
                            <td
                                style="
                                vertical-align: middle;
                                color: transparent;
                                font-size: 0;
                                "
                                width="16"
                            >
                                &#8203;
                            </td>
                            <td
                                align="left"
                                class="u"
                                style="
                                padding: 0;
                                height: auto;
                                word-wrap: break-word;
                                vertical-align: middle;
                                "
                                width="auto"
                            >
                                <table
                                border="0"
                                cellpadding="0"
                                cellspacing="0"
                                width="100%"
                                >
                                <tr>
                                    <td align="left" width="100%">
                                    <div
                                        style="
                                        font-family: 'Open Sans',
                                            'Arial', sans-serif;
                                        font-size: 14px;
                                        font-weight: 500;
                                        line-height: 157%;
                                        text-align: left;
                                        color: #090c02;
                                        "
                                    >
                                        <p
                                        style="
                                            margin: 0;
                                            mso-line-height-alt: 22px;
                                            font-size: 14px;
                                            line-height: 157%;
                                        "
                                        >
                                        Bounding Box Selection
                                        </p>
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
                        <td
                        style="
                            font-size: 0;
                            padding: 0;
                            word-break: break-word;
                        "
                        >
                        <div style="height: 8px; line-height: 8px">
                            &#8202;
                        </div>
                        </td>
                    </tr>
                    <tr>
                        <td
                        align="center"
                        class="tr-0"
                        style="
                            background: transparent;
                            font-size: 0;
                            padding: 0px 48px 0px 48px;
                            word-break: break-word;
                        "
                        >
                        <table
                            cellpadding="0"
                            cellspacing="0"
                            width="100%"
                            border="0"
                            style="
                            color: #000000;
                            line-height: normal;
                            table-layout: fixed;
                            width: 100%;
                            border: none;
                            "
                        >
                            <tr>
                            <td
                                align="left"
                                class="u"
                                style="
                                padding: 0;
                                height: auto;
                                word-wrap: break-word;
                                vertical-align: middle;
                                "
                                width="500"
                            >
                                <table
                                border="0"
                                cellpadding="0"
                                cellspacing="0"
                                width="100%"
                                >
                                <tr>
                                    <td align="left" width="100%">
                                    <div
                                        style="
                                        font-family: 'Open Sans',
                                            'Arial', sans-serif;
                                        font-size: 14px;
                                        font-weight: 400;
                                        line-height: 157%;
                                        text-align: left;
                                        color: #3c3c3c;
                                        "
                                    >
                                        <p
                                        style="
                                            margin: 0;
                                            mso-line-height-alt: 22px;
                                            font-size: 14px;
                                            line-height: 157%;
                                        "
                                        >
                                        N: {bbox.max_lat}
                                        </p>
                                    </div>
                                    </td>
                                </tr>
                                </table>
                            </td>
                            </tr>
                            <tr>
                            <td
                                style="
                                font-size: 0;
                                padding: 0;
                                padding-bottom: 0;
                                word-break: break-word;
                                color: transparent;
                                "
                                aria-hidden="true"
                            >
                                <div
                                style="
                                    height: 8px;
                                    line-height: 8px;
                                "
                                >
                                &#8203;
                                </div>
                            </td>
                            </tr>
                            <tr>
                            <td
                                align="left"
                                class="u"
                                style="
                                padding: 0;
                                height: auto;
                                word-wrap: break-word;
                                vertical-align: middle;
                                "
                                width="500"
                            >
                                <table
                                border="0"
                                cellpadding="0"
                                cellspacing="0"
                                width="100%"
                                >
                                <tr>
                                    <td align="left" width="100%">
                                    <div
                                        style="
                                        font-family: 'Open Sans',
                                            'Arial', sans-serif;
                                        font-size: 14px;
                                        font-weight: 400;
                                        line-height: 157%;
                                        text-align: left;
                                        color: #3c3c3c;
                                        "
                                    >
                                        <p
                                        style="
                                            margin: 0;
                                            mso-line-height-alt: 22px;
                                            font-size: 14px;
                                            line-height: 157%;
                                        "
                                        >
                                        S: {bbox.min_lat}
                                        </p>
                                    </div>
                                    </td>
                                </tr>
                                </table>
                            </td>
                            </tr>
                            <tr>
                            <td
                                style="
                                font-size: 0;
                                padding: 0;
                                padding-bottom: 0;
                                word-break: break-word;
                                color: transparent;
                                "
                                aria-hidden="true"
                            >
                                <div
                                style="
                                    height: 8px;
                                    line-height: 8px;
                                "
                                >
                                &#8203;
                                </div>
                            </td>
                            </tr>
                            <tr>
                            <td
                                align="left"
                                class="u"
                                style="
                                padding: 0;
                                height: auto;
                                word-wrap: break-word;
                                vertical-align: middle;
                                "
                                width="500"
                            >
                                <table
                                border="0"
                                cellpadding="0"
                                cellspacing="0"
                                width="100%"
                                >
                                <tr>
                                    <td align="left" width="100%">
                                    <div
                                        style="
                                        font-family: 'Open Sans',
                                            'Arial', sans-serif;
                                        font-size: 14px;
                                        font-weight: 400;
                                        line-height: 157%;
                                        text-align: left;
                                        color: #3c3c3c;
                                        "
                                    >
                                        <p
                                        style="
                                            margin: 0;
                                            mso-line-height-alt: 22px;
                                            font-size: 14px;
                                            line-height: 157%;
                                        "
                                        >
                                        W: {bbox.min_lon}
                                        </p>
                                    </div>
                                    </td>
                                </tr>
                                </table>
                            </td>
                            </tr>
                            <tr>
                            <td
                                style="
                                font-size: 0;
                                padding: 0;
                                padding-bottom: 0;
                                word-break: break-word;
                                color: transparent;
                                "
                                aria-hidden="true"
                            >
                                <div
                                style="
                                    height: 8px;
                                    line-height: 8px;
                                "
                                >
                                &#8203;
                                </div>
                            </td>
                            </tr>
                            <tr>
                            <td
                                align="left"
                                class="u"
                                style="
                                padding: 0;
                                height: auto;
                                word-wrap: break-word;
                                vertical-align: middle;
                                "
                                width="500"
                            >
                                <table
                                border="0"
                                cellpadding="0"
                                cellspacing="0"
                                width="100%"
                                >
                                <tr>
                                    <td align="left" width="100%">
                                    <div
                                        style="
                                        font-family: 'Open Sans',
                                            'Arial', sans-serif;
                                        font-size: 14px;
                                        font-weight: 400;
                                        line-height: 157%;
                                        text-align: left;
                                        color: #3c3c3c;
                                        "
                                    >
                                        <p
                                        style="
                                            margin: 0;
                                            mso-line-height-alt: 22px;
                                            font-size: 14px;
                                            line-height: 157%;
                                        "
                                        >
                                        E: {bbox.max_lon}
                                        </p>
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
                <!--[if mso | IE]></td></tr></table><![endif]-->
            </td>
            </tr>
        </tbody>
        </table>
    </div>
    """

    return one_bbox_div
