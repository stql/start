/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.templeton;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.http.HttpStatus;

/**
 * Map all exceptions to the Jersey response.  This lets us have nice
 * results in the error body.
 */
@Provider
public class CatchallExceptionMapper
    implements ExceptionMapper<Exception> {
    private static final Log LOG = LogFactory.getLog(CatchallExceptionMapper.class);

    public Response toResponse(Exception e) {
        LOG.error(e.getMessage(), e);
        return SimpleWebException.buildMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, null, e.getMessage());
    }
}
