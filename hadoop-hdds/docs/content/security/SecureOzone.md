---
title: "Securing Ozone"
date: "2019-04-03"
summary: Overview of Ozone security concepts and steps to secure Ozone Manager and SCM.
weight: 1
menu:
   main:
      parent: Security
icon: tower
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->


# Kerberos

Ozone depends on [Kerberos](https://web.mit.edu/kerberos/) to make the
clusters secure. Historically, HDFS has supported running in an isolated
secure networks where it is possible to deploy without securing the cluster.

This release of Ozone follows that model, but soon will move to _secure by
default._  Today to enable security in ozone cluster, we need to set the
configuration **ozone.security.enabled** to _true_ and **hadoop.security.authentication**
to _kerberos_.

Property|Value
----------------------|---------
ozone.security.enabled| _true_
hadoop.security.authentication| _kerberos_

# Tokens #

Ozone uses a notion of tokens to avoid overburdening the Kerberos server.
When you serve thousands of requests per second, involving Kerberos might not
work well. Hence once an authentication is done, Ozone issues delegation
tokens and block tokens to the clients. These tokens allow applications to do
specified operations against the cluster, as if they have kerberos tickets
with them. Ozone supports following kinds of tokens.

### Delegation Token ###
Delegation tokens allow an application to impersonate a users kerberos
credentials. This token is based on verification of kerberos identity and is
issued by the Ozone Manager. Delegation tokens are enabled by default when
security is enabled.

### Block Token ###

Block tokens allow a client to read or write a block. This is needed so that
data nodes know that the user/client has permission to read or make
modifications to the block.

### S3AuthInfo ###

S3 uses a very different shared secret security scheme. Ozone supports the AWS Signature Version 4 protocol,
and from the end users perspective Ozone's S3 feels exactly like AWS S3.

The S3 credential tokens are called S3 auth info in the code. These tokens are
also enabled by default when security is enabled.


Each of the service daemons that make up Ozone needs a  Kerberos service
principal name and a corresponding [kerberos key tab](https://web.mit.edu/kerberos/krb5-latest/doc/basic/keytab_def.html) file.

All these settings should be made in ozone-site.xml.

<div class="card-group">
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">Storage Container Manager</h3>
      <p class="card-text">
      <br>
        SCM requires two Kerberos principals, and the corresponding key tab files
        for both of these principals.
      <br>
      <table class="table table-dark">
        <thead>
          <tr>
            <th scope="col">Property</th>
            <th scope="col">Description</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>hdds.scm.kerberos.principal</th>
            <td>The SCM service principal. <br/> e.g. scm/_HOST@REALM.COM</td>
          </tr>
          <tr>
            <td>hdds.scm.kerberos.keytab.file</th>
            <td>The keytab file used by SCM daemon to login as its service principal.</td>
          </tr>
          <tr>
            <td>hdds.scm.http.auth.kerberos.principal</th>
            <td>SCM http server service principal if SPNEGO is enabled for SCM http server.</td>
          </tr>
          <tr>
            <td>hdds.scm.http.auth.kerberos.keytab</th>
            <td>The keytab file used by SCM http server to login as its service principal if SPNEGO is enabled for SCM http server</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">Ozone Manager</h3>
      <p class="card-text">
      <br>
        Like SCM, OM also requires two Kerberos principals, and the
        corresponding key tab files for both of these principals.
      <br>
      <table class="table table-dark">
        <thead>
          <tr>
            <th scope="col">Property</th>
            <th scope="col">Description</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>ozone.om.kerberos.principal</th>
            <td>The OzoneManager service principal. <br/> e.g. om/_HOST@REALM.COM</td>
          </tr>
          <tr>
            <td>ozone.om.kerberos.keytab.file</th>
            <td>The keytab file used by OM daemon to login as its service principal.</td>
          </tr>
          <tr>
            <td>ozone.om.http.auth.kerberos.principal</th>
            <td>Ozone Manager http server service principal if SPNEGO is enabled for om http server.</td>
          </tr>
          <tr>
            <td>ozone.om.http.auth.kerberos.keytab</th>
            <td>The keytab file used by OM http server to login as its service principal if SPNEGO is enabled for om http server.</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <div class="card-body">
      <h3 class="card-title">S3 Gateway</h3>
      <p class="card-text">
      <br>
        S3 gateway requires one service principal and here the configuration values
        needed in the ozone-site.xml.
      <br>
      <table class="table table-dark">
        <thead>
          <tr>
            <th scope="col">Property</th>
            <th scope="col">Description</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>ozone.s3g.kerberos.principal</th>
            <td>S3 Gateway principal. <br/> e.g. s3g/_HOST@REALM</td>
          </tr>
          <tr>
            <td>ozone.s3g.kerberos.keytab.file</th>
            <td>The keytab file used by S3 gateway. <br/> e.g. /etc/security/keytabs/s3g.keytab</td>
          </tr>
          <tr>
            <td>ozone.s3g.http.auth.kerberos.principal</th>
            <td>S3 Gateway principal if SPNEGO is enabled for S3 Gateway http server. <br/> e.g. HTTP/_HOST@EXAMPLE.COM</td>
          </tr>
          <tr>
            <td>ozone.s3g.http.auth.kerberos.keytab</th>
            <td>The keytab file used by S3 gateway if SPNEGO is enabled for S3 Gateway http server.</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</div>
