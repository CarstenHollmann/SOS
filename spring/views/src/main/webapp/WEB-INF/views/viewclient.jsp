<%--

    Copyright (C) 2012-2016 52°North Initiative for Geospatial Open Source
    Software GmbH

    This program is free software; you can redistribute it and/or modify it
    under the terms of the GNU General Public License version 2 as published
    by the Free Software Foundation.

    If the program is linked with libraries which are licensed under one of
    the following licenses, the combination of the program with the linked
    library is not considered a "derivative work" of the program:

        - Apache License, version 2.0
        - Apache Software License, version 1.0
        - GNU Lesser General Public License, version 3
        - Mozilla Public License, versions 1.0, 1.1 and 2.0
        - Common Development and Distribution License (CDDL), version 1.0

    Therefore the distribution of the program linked with libraries licensed
    under the aforementioned licenses, is permitted by the copyright holders
    if the distribution is compliant with both the GNU General Public
    License version 2 and the aforementioned licenses.

    This program is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
    Public License for more details.

--%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions"%>
<jsp:include page="common/header.jsp">
    <jsp:param name="activeMenu" value="viewclient" />
</jsp:include>

<%--
    JSP parameters
 --%>
<jsp:include page="common/logotitle.jsp">
    <jsp:param name="title" value="52&deg;North SOS.js based View Client" />
    <jsp:param name="leadParagraph" value="A lightweight JavaScript SOS client." />
</jsp:include>

<script type="text/javascript">
    /* redirect from "viewclient/"" to "viewclient" */
    if (window.location.pathname.slice(-1) === "/") {
        window.location.href = window.location.href.slice(0, -1);
    }
</script>

<div>
    <p>A demonstration of the sos-js application, a JavaScript client to display and analyse time series data provided via standardized OGC Sensor Observation Service instances. To learn more about the project go to the project page: <a title="sos-js project page" href="https://github.com/52North/sos-js">https://github.com/52North/sos-js</a></p>
</div>

<iframe src="sos-js" width="940" height="600" name="sosjsframe" marginheight="0" marginwidth="0" frameborder="0">
	<p>Your browser does not support frames, go to <a href="sos-js">SOS.js page</a>.</p>
</iframe>

<jsp:include page="common/footer.jsp" />