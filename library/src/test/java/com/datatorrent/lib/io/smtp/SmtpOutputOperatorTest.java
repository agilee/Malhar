/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.smtp;

import com.datatorrent.api.*;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;
import com.icegreen.greenmail.util.ServerSetupTest;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmtpOutputOperatorTest
{

  String subject = "ALERT!";
  String content = "This is an SMTP operator test {}";
  String from = "jenkins@datatorrent.com";
  String to = "jenkins@datatorrent.com";
  String cc = "jenkinsCC@datatorrent.com";
  GreenMail greenMail = null;
  SmtpIdempotentOutputOperator node;

  Map<String, String> data;

  public class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;
    IdempotentStorageManager.FSIdempotentStorageManager manager = new IdempotentStorageManager.FSIdempotentStorageManager();
    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.DAGContext.APPLICATION_ID, "FileInputOperatorTest");
      context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
      node = new SmtpIdempotentOutputOperator();
      greenMail = new GreenMail(ServerSetupTest.ALL);
      greenMail.start();
      node.setFrom(from);
      node.setContent(content);
      node.setSmtpHost("127.0.0.1");
      node.setSmtpPort(ServerSetupTest.getPortOffset() + ServerSetup.SMTP.getPort());
      node.setSmtpUserName(from);
      node.setSmtpPassword("<password>");
      data = new HashMap<String, String>();
      data.put("alertkey", "alertvalue");

    manager.setRecoveryPath(testMeta.dir + "/recovery");
    manager.setup(testMeta.context);
    node.setIdempotentStorageManager(manager);

    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(this.dir));
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @After
  public void teardown() throws Exception
  {
    greenMail.stop();
    Thread.sleep(1000);
  }

  @Test
  public void testSmtpOutputNode() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to + "," + cc);
    recipients.put("cc", cc);
    node.setRecipients(recipients);
    node.setSubject("hello");
    node.setup(testMeta.context);
    node.beginWindow(1000);
    String data = "First test message";
    node.input.process(data);
    node.endWindow();
    node.teardown();

    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(3, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getRecipients(Message.RecipientType.TO)[0].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.TO)[1].toString());
    Assert.assertEquals(cc, messages[0].getRecipients(Message.RecipientType.CC)[0].toString());
  }

  @Test
  public void test() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();
    recipients.put("to", to);
    node.setRecipients(recipients);
    node.setup(testMeta.context);
    node.activate(testMeta.context);
    node.beginWindow(0);
    String data = "First test message";
    node.input.process(data);
    node.endWindow();
    node.teardown();
    Assert.assertTrue(greenMail.waitForIncomingEmail(10000, 1));
    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(1, messages.length);
    String receivedContent = messages[0].getContent().toString().trim();
    String expectedContent = content.replace("{}", data.toString()).trim();

    Assert.assertTrue(expectedContent.equals(receivedContent));
    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getAllRecipients()[0].toString());
  }

  @Test
  public void testProperties() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.subject", subject);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.content", content);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.from", from);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpHost", "127.0.0.1");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpUserName", from);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.smtpPassword", "<password>");
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.recipients.TO", to + "," + cc);
    conf.set(StreamingApplication.DT_PREFIX + "operator.o1.prop.recipients.CC", cc);

    final AtomicReference<SmtpIdempotentOutputOperator> o1 = new AtomicReference<SmtpIdempotentOutputOperator>();
    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        o1.set(dag.addOperator("o1", new SmtpIdempotentOutputOperator()));
      }

    };

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(app, conf);
    Assert.assertEquals("checking TO list", to + "," + cc, o1.get().getRecipients().get("TO"));
    Assert.assertEquals("checking CC list", cc, o1.get().getRecipients().get("CC"));
  }

  @Test
  public void testIdempotency() throws Exception
  {
    Map<String, String> recipients = Maps.newHashMap();

    recipients.put("to", to);
    node.setup(testMeta.context);
    node.activate(testMeta.context);
    node.setRecipients(recipients);
    node.setSubject("hello");
    for (long wid = 0; wid < 10; wid++) {
      if(wid == 2){
        Thread.sleep(500);
        node.handleIdleTime();
      }
      Thread.sleep(1000);
      if (wid == 7) {
        SmtpIdempotentOutputOperator newOp = TestUtils.clone(new Kryo(), node);
        node.teardown();
        newOp.setup(testMeta.context);
        newOp.activate(testMeta.context);
        newOp.beginWindow(4);
        String inputTest = 4 + "test message";
        newOp.input.process(inputTest);
        newOp.endWindow();
        Thread.sleep(1000);
        newOp.beginWindow(5);
        inputTest = 5 + "test message";
        newOp.input.process(inputTest);
        newOp.endWindow();

        newOp.beginWindow(6);
        inputTest = 6 + "test message";
        newOp.input.process(inputTest);
        newOp.endWindow();

        newOp.beginWindow(7);
        inputTest = 7 + "test message";
        newOp.input.process(inputTest);
        newOp.endWindow();

        newOp.beginWindow(8);
        inputTest = 8 + "test message";
        newOp.input.process(inputTest);
        newOp.endWindow();
        Thread.sleep(1000);
        newOp.teardown();
        break;
      }
      node.beginWindow(wid);
      String input = wid + "test message";
      node.input.process(input);
      Thread.sleep(1000);
      node.endWindow();
    }
    Assert.assertTrue(greenMail.waitForIncomingEmail(5000, 1));

    MimeMessage[] messages = greenMail.getReceivedMessages();
    Assert.assertEquals(9, messages.length);
    for (int i = 0; i < messages.length; i++) {
      String receivedContent = messages[i].getContent().toString().trim();
      logger.debug("received content is {}", receivedContent);
      String expectedContent = content.replace("{}", i + "test message").trim();
      Assert.assertTrue(expectedContent.equals(receivedContent));
    }

    Assert.assertEquals(from, ((InternetAddress)messages[0].getFrom()[0]).getAddress());
    Assert.assertEquals(to, messages[0].getRecipients(Message.RecipientType.TO)[0].toString());
  }

  private static final Logger logger = LoggerFactory.getLogger(SmtpOutputOperatorTest.class);
}