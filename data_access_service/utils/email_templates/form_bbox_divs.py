from data_access_service.models.bounding_box import BoundingBox


def form_bbox_divs(bboxes: [BoundingBox]):
    all_divs = ""
    for bbox in bboxes:
        all_divs += __form_bbox_div(bbox)
    return all_divs


def __form_bbox_div(bbox: BoundingBox):

    bbox_img = png_to_base64(
        "data_access_service/utils/email_templates/img/1419180c04931cd1e419efce2690d5b8.png"
    )
    one_bbox_div = f"""
    <!--[if mso | IE]></td><td style="vertical-align:middle;width:600px;"><![endif]-->
    <div class="c h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:middle;width:100%;">
    <table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border:none;vertical-align:middle;" width="100%">
    <tbody>
    <tr>
    <td align="center" class="tr-0" style="background:transparent;font-size:0;padding:0;word-break:break-word;">
    <table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;line-height:normal;table-layout:fixed;width:100%;border:none;">
    <tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="32"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"> <img alt width="32" style="display:block;width:32px;height:32px;" src="data:image/png;base64,{bbox_img}"></td></tr></table></td><td style="vertical-align:middle;color:transparent;font-size:0;" width="16">&#8203;</td><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="auto"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 500; line-height: 157%; text-align: left; color: #090c02"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">Bounding Box Selection</p></div></td></tr></table></td></tr>
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
    <tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">N: {bbox.max_lat}</p></div></td></tr></table></td></tr><tr><td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true"><div style="height:8px;line-height:8px;">&#8203;</div></td></tr><tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">S: {bbox.min_lat}</p></div></td></tr></table></td></tr><tr><td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true"><div style="height:8px;line-height:8px;">&#8203;</div></td></tr><tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">W: {bbox.min_lon}</p></div></td></tr></table></td></tr><tr><td style="font-size:0;padding:0;padding-bottom:0;word-break:break-word;color:transparent;" aria-hidden="true"><div style="height:8px;line-height:8px;">&#8203;</div></td></tr><tr><td align="left" class="u" style="padding:0;height:auto;word-wrap:break-word;vertical-align:middle;" width="500"><table border="0" cellpadding="0" cellspacing="0" width="100%"><tr><td align="left" width="100%"><div style="font-family: 'Open Sans', Arial, sans-serif; font-size: 14px; font-weight: 400; line-height: 157%; text-align: left; color: #3c3c3c"><p style="Margin:0;mso-line-height-alt:22px;font-size:14px;line-height:157%;">E: {bbox.max_lon}</p></div></td></tr></table></td></tr>
    </table>
    </td>
    </tr>
    </tbody>
    </table>
    </div>
    <!--[if mso | IE]></td><td style="vertical-align:top;width:308px;"><![endif]-->
    <div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
    <tbody>
    <tr>
    <td style="vertical-align:top;padding:0;">
    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
    <tbody>
    </tbody>
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
    <!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div style="margin:0px auto;max-width:1264px;">
    <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;">
    <tbody>
    <tr>
    <td style="direction:ltr;font-size:0;padding:0;text-align:center;">
    <!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:1264px;"><![endif]-->
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
    <!--[if mso | IE]></td></tr></table><![endif]-->
    </td>
    </tr>
    </tbody>
    </table>
    </div>
    <!--[if mso | IE]></td></tr></table></td></tr><tr><td width="1280px"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:1264px;" width="1264"><tr><td style="line-height:0;font-size:0;mso-line-height-rule:exactly;"><![endif]-->
    <div class="r e y" style="background:#fffffe;background-color:#fffffe;margin:0px auto;max-width:1264px;">
    <table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#fffffe;background-color:#fffffe;width:100%;">
    <tbody>
    <tr>
    <td style="border:none;direction:ltr;font-size:0;padding:10px 24px 10px 24px;text-align:center;">
    <!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td style="vertical-align:top;width:308px;"><![endif]-->
    <div class="h7 h" style="font-size:0;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;">
    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
    <tbody>
    <tr>
    <td style="vertical-align:top;padding:0;">
    <table border="0" cellpadding="0" cellspacing="0" role="presentation" width="100%">
    <tbody>
    </tbody>
    </table>
    </td>
    </tr>
    </tbody>
    </table>
    </div>

    """
