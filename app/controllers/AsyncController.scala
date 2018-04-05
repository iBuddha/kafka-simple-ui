package controllers

import java.sql.Timestamp
import javax.inject._

import akka.actor.{ActorSystem, Props}
import kafka.authorization.manager.actor._
import kafka.authorization.manager.model.form._
import play.api.i18n._
import play.api.mvc._
import akka.pattern._
import akka.util.Timeout
import kafka.security.auth._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import com.typesafe.config.ConfigFactory
import kafka.admin.BrokerMetadata
import kafka.authorization.manager.actor.AdminActor.{ResetGroupOffsets, ResetGroupTopicOffset, TriggerRebalance}
import kafka.authorization.manager.model.request._
import kafka.authorization.manager.model.{GroupDescription, NewTopicWithAssignment, ResourceAcl, TopicPartition}
import kafka.server.{KafkaConfig, ProduceMetadata}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory
import play.api.data.Form
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}
import scala.collection.immutable.ListMap

/**
  * This controller creates an `Action` that demonstrates how to write
  * simple asynchronous code in a controller. It uses a timer to
  * asynchronously delay sending a response for 1 second.
  *
  * @param actorSystem We need the `ActorSystem`'s `Scheduler` to
  *                    run code after a delay.
  * @param exec        We need an `ExecutionContext` to execute our
  *                    asynchronous code.
  */
@Singleton
class AsyncController @Inject()(actorSystem: ActorSystem)(val messagesApi: MessagesApi)(implicit exec: ExecutionContext) extends Controller with I18nSupport {

  import AsyncControllerHelper._

  private val logger = LoggerFactory.getLogger(classOf[AsyncController])

  private implicit val timeout = Timeout(15 seconds)
  private implicit val implicitActorSystem = actorSystem

  private val bootstrapServers = ConfigFactory.load().getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
  private val zkConnect = ConfigFactory.load().getString(KafkaConfig.ZkConnectProp)
  private val authorizerSupervisorActor = supervisorActor(Props[AuthorizerActor], "authorizer-actor")
  private val consumerGroupSupervisorActor = supervisorActor(Props(classOf[ConsumerGroupActor], bootstrapServers), "consumer-group-actor")
  private val messageLookupSupervisorActor = supervisorActor(Props(new MessageActor(bootstrapServers, timeout.duration.toMillis)), "message-actor")
  private val metadataSupervisorActor = supervisorActor(Props(new MetadataActor(bootstrapServers)), "metadata-actor")

  private val offsetLookupSupervisorActor = supervisorActor(Props(new OffsetLookupActor(bootstrapServers)), "offset-actor")

  private val latestMessageSupervisorActor = supervisorActor(Props(new SearchMessageActor(messageLookupSupervisorActor.path, offsetLookupSupervisorActor.path, metadataSupervisorActor.path)), "latestMessage-actor")

  private val produceSupervisorActor = supervisorActor(Props(new ProducerActor(bootstrapServers, timeout.duration.toMillis)), "producer-actor")


  private val topicCommandSupervisorActor = supervisorActor(Props(new TopicCommandActor(zkConnect)), "topic-command-actor")
  private val adminSupervisorActor = supervisorActor(Props(new AdminActor(bootstrapServers, offsetLookupSupervisorActor)), "admin-actor")

  //  val offsetLookupActor = actorSystem.actorOf(Props(new OffsetLookupActor(bootstrapServers)), "offset-actor")
  //  val messageLookupActor = actorSystem.actorOf(Props(new MessageActor(bootstrapServers, requestTimeout)), "message-actor")
  //  val metadataActor = actorSystem.actorOf(Props(new MetadataActor(bootstrapServers)), "metadata-actor")
  //  val latestMessageActor = actorSystem.actorOf(Props(new SearchMessageActor(messageLookupActor.path, offsetLookupActor.path, metadataActor.path)), "currentMessageActor")


  private val addResultUrl = routes.AsyncController.addResult()
  private val principalFormPostUrl = routes.AsyncController.findPrincipalAcls()
  private val resourceFormPostUrl = routes.AsyncController.findResourceAcls()
  private val latestMessagePostUrl = routes.AsyncController.latestMessagePost()


  def index = Action {
    Ok(views.html.index())
  }

  def listAllAcls = Action.async {
    (authorizerSupervisorActor ? GetAllAclsReq).mapTo[AllAcls].map { allAcls => {
      Ok(views.html.allAcls(allAcls.acls))
    }
    }
  }


  def addAcl = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.user(AclRecordForm.form, addResultUrl, None))
  }


  def addResult = Action.async { implicit request =>
    val formValidationResult = AclRecordForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.user(formWithErrors, addResultUrl, Some(false))))
    }, {
      //      aclRecord => Ok(views.html.user(aclForm, addAclUrl))
      aclRecord => {
        val acl = aclRecord.acl
        val resource = aclRecord.resource
        val addAclRequest = AddAclsReq(Set(acl), resource)
        (authorizerSupervisorActor ? addAclRequest).flatMap {
          case SuccessfullyAdded =>
            (authorizerSupervisorActor ? GetResourceAclsReq(resource))
              .mapTo[AclsForResource]
              .map(acls =>
                Ok(views.html.user(formValidationResult,
                  addResultUrl,
                  Some(true),
                  Some(Map(acls.resource -> acls.acls)
                  ))))
        }
      }
    })
  }

  def deleteAcl(acl: Acl, resource: Resource) = Action.async { implicit request =>
    val deletedResultFuture = (authorizerSupervisorActor ? RemoveAclsReq(Set(acl), resource))
    val currentAclsFuture = getResourceAcl(resource)
    for (deleteResult <- deletedResultFuture;
         currentAcls <- currentAclsFuture) yield {
      val message = deleteResult match {
        case SuccessfullyRemoved => s"Successfully deleted ACL $acl from $resource"
        case FailedToRemove(reason) => s"Failed to remove ACL $acl from $resource because $reason"
      }

      Ok(views.html.deleteResult(message,
        SelectResourceForm.getForm(resource),
        resourceFormPostUrl,
        Some((currentAcls.resource, currentAcls.acls))))
    }
  }


  def deleteAcl = Action.async { implicit request =>
    logger.info(request.body.toString)
    import kafka.authorization.manager.model.AuthJsonUtils._
    val json = request.body.asJson.get.toString
    val ResourceAcl(resource, acl) = Json.parse(json).as[ResourceAcl](resourceAclReads)
    val deletedResultFuture = (authorizerSupervisorActor ? RemoveAclsReq(Set(acl), resource))
    val currentAclsFuture = getResourceAcl(resource)
    for (deleteResult <- deletedResultFuture;
         currentAcls <- currentAclsFuture) yield {
      val message = deleteResult match {
        case SuccessfullyRemoved => s"Successfully deleted ACL $acl from $resource"
        case FailedToRemove(reason) => s"Failed to remove ACL $acl from $resource because $reason"
      }
      Ok(message)
    }
  }

  def findResourceAcls = Action.async { implicit request =>
    val formValidationResult = SelectResourceForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.resourceAcl(formWithErrors, resourceFormPostUrl, None)))
    }, {
      selectResource => getResourceView(selectResource.resourceName, selectResource.kafkaResourceType, request)
    })
  }

  def findPrincipalAcls = Action.async { implicit request =>
    val formValidationResult = SelectPrincipalForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.principalAcl(formWithErrors, principalFormPostUrl, None)))
    }, { selectPrincipal => {
      val principal = selectPrincipal.toKafkaPrincipal()
      (authorizerSupervisorActor ? GetPrincipalAclsReq(principal)).mapTo[AclsOfPrincipal].map { aclsOfPrincipal => {
        Ok(views.html.principalAcl(formValidationResult, principalFormPostUrl, Some(principal, aclsOfPrincipal.acls)))
      }
      }
    }
    })
  }

  def getTopicAcls(topicName: String) = Action.async { implicit request =>
    getResourceView(topicName, Topic, request)
  }

  def getGroupAcls(groupName: String) = Action.async { implicit request =>
    getResourceView(groupName, Group, request)
  }

  def getClusterAcls() = Action.async { implicit request =>
    getResourceView("", Cluster, request)
  }


  private def getResourceView(resourceName: String, resourceType: ResourceType, request: Request[AnyContent]): Future[Result] = {
    implicit val req = request
    val resourceForm = SelectResourceForm.form.fill(SelectResourceForm(resourceName, resourceType.name))
    getResourceAcl(resourceForm.get.toResource())
      .map { aclsForResource => {
        Ok(views.html.resourceAcl(resourceForm, resourceFormPostUrl, Some(aclsForResource.resource, aclsForResource.acls)))
      }
      }
  }

  private def getResourceAcl(resource: Resource): Future[AclsForResource] = {
    (authorizerSupervisorActor ? GetResourceAclsReq(resource)).mapTo[AclsForResource]
  }


  def resourceQuery = Action { implicit request =>
    Ok(views.html.resourceAcl(SelectResourceForm.form, resourceFormPostUrl, None))
  }


  def principalQuery = Action { implicit request =>
    Ok(views.html.principalAcl(SelectPrincipalForm.form, principalFormPostUrl, None))
  }


  //  def describeGroupGet(groupId: String) = Action.async { implicit request =>
  //    describeGroup(groupId).map(result => Ok(views.html.groupDescription(ConsumerGroupForm.form.fill(ConsumerGroupForm(groupId)), Some(result))))
  //  }

  def describeGroupGet = Action { implicit request =>
    Ok(views.html.groupDescription(ConsumerGroupForm.form, None))
  }

  private val middleLevelTimeout = Timeout(30 seconds)

  private def describeGroup(groupId: String, form: Form[ConsumerGroupForm], errorMessage: Option[String] = None)(implicit request: Request[AnyContent]): Future[Result] = {
    (consumerGroupSupervisorActor.ask(DescribeConsumerGroupRequest(-1, groupId))(middleLevelTimeout)).mapTo[Try[GroupDescription]].map {
      case Success(description) =>
        Ok(views.html.groupDescription(form, Some(description), errorMessage))
      case Failure(e) => Ok(views.html.groupDescription(form, None, Some(e.toString)))
    }
  }

  private def describeConsumerGroup(groupId: String, errorMessage: Option[String] = None)(implicit request: Request[AnyContent]): Future[Result] = {
    val form = ConsumerGroupForm.getForm(groupId)
    describeGroup(groupId, form, errorMessage)
  }

  def describeGroupPost = Action.async { implicit request =>
    val formValidationResult = ConsumerGroupForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.groupDescription(formWithErrors, None)))
    }, { form =>
      describeGroup(form.group, formValidationResult)
    })
  }


  private def getLatestMessage(topic: String): Future[RecentlyMessageResponse] = {
    latestMessageSupervisorActor.ask(RecentlyMessageRequest(-1, topic))(middleLevelTimeout).mapTo[RecentlyMessageResponse]
  }

  def latestMessageGet = Action { implicit request =>
    Ok(views.html.latestMessage(TopicForm.form, latestMessagePostUrl, None))
  }

  def latestMessagePost = Action.async { implicit request =>
    val formValidationResult = TopicForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.latestMessage(formWithErrors, latestMessagePostUrl, None)))
    }, { form =>
      getLatestMessage(form.topic).map(result => {
        import kafka.authorization.manager.utils.TextEncoder._
        result.result match {
          case Success(messages) => Left(messages.map {
            case Success(message) => Left(StringMessage(
              key = encodeUTF8(message.key),
              value = encodeUTF8(message.value),
              ts = new Timestamp(message.ts).toString,
              partition = message.partition,
              offset = message.offset
            ))
            case Failure(e) => Right(e.getMessage)
          })
          case Failure(exception) => Right(exception.getMessage)
        }
      }).map(messages => Ok(views.html.latestMessage(formValidationResult, latestMessagePostUrl, Some(messages))))
    })
  }

  //  def searchOffset(topic: String, timestamp: Long): Future[FindTopicOffsetResponse] = {
  //    val mockPartitionOffsetMap: Map[Int, OffsetBehindTs] = Map(0 -> OffsetBehindTs(100, System.currentTimeMillis()))
  //    Future.successful(FindTopicOffsetResponse(FindTopicOffsetRequest(-1, "test", 0), Success(mockPartitionOffsetMap)))
  //  }


  def topicOffsetGet = Action { implicit request =>
    //    Ok(views.html.)
    Ok(views.html.topicOffset(SelectTopicOffsetForm.form, None))
  }

  def topicOffsetPost = Action.async { implicit request =>
    val formValidationResult = SelectTopicOffsetForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.topicOffset(formWithErrors, None)))
    }, { form =>
      (offsetLookupSupervisorActor ? FindTopicOffsetRequest(-1, form.topic, form.time.getMillis)).mapTo[FindTopicOffsetResponse].map(response => {
        response.partitionOffset match {
          case Failure(e) => Ok(views.html.topicOffset(formValidationResult, None, Some(e.getMessage)))
          case Success(offsets) =>
            val sorted = ListMap(offsets.toSeq.sortBy(_._1): _*)
            Ok(views.html.topicOffset(formValidationResult, Some(sorted)))
        }
      })
    })
  }

  def topicOffsetDiffGet = Action { implicit request =>
    //    Ok(views.html.)
    Ok(views.html.topicOffsetDiff(SelectTopicOffsetDiffForm.form, None))
  }

  def topicOffsetDiffPost = Action.async { implicit request =>
    val formValidationResult = SelectTopicOffsetDiffForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.topicOffsetDiff(formWithErrors, None)))
    }, { form =>
      val beginFuture = (offsetLookupSupervisorActor ? FindTopicOffsetRequest(-1, form.topic, form.from.getMillis)).mapTo[FindTopicOffsetResponse]

      beginFuture.flatMap(beginResponse => beginResponse.partitionOffset match {
        case Failure(e) => Future.successful(Ok(views.html.topicOffsetDiff(formValidationResult, None, Some(e.getMessage))))
        case Success(beginOffsets) => {
          (offsetLookupSupervisorActor ? FindTopicOffsetRequest(-1, form.topic, form.to.getMillis)).mapTo[FindTopicOffsetResponse].map(endResponse => endResponse.partitionOffset match {
            case Failure(e) => Ok(views.html.topicOffsetDiff(formValidationResult, None, Some(e.getMessage)))
            case Success(endOffsets) => Ok(views.html.topicOffsetDiff(formValidationResult, Some(offsetDiff(beginOffsets, endOffsets))))
          })
        }

      })
      //      val endFuture = (offsetLookupSupervisorActor ? FindTopicOffsetRequest(-1, form.topic, form.to.getMillis)).mapTo[FindTopicOffsetResponse]
      //
      //      for(begin <- beginFuture; end <- endFuture) yield {
      //        if(begin.partitionOffset.isSuccess && end.partitionOffset.isSuccess){
      //
      //        } else {
      //          val message = (begin.partitionOffset match{
      //            case Failure(e) =>   s"failed to get begin offsets, because: ${e.getMessage}"
      //            case _ => ""
      //          }) + (end.partitionOffset match {
      //            case Failure(e) =>   s"failed to get end offsets, because: ${e.getMessage}"
      //            case _ => ""
      //          })
      //         Future.successful(Ok(views.html.topicOffsetDiff(formValidationResult, None, Some(message))))
      //        }
      //      }

      //      Future.successful(Ok(""))
      //      (response => {
      //        response.partitionOffset match {
      //          case Failure(e) => Ok(views.html.topicOffset(formValidationResult, None, Some(e.getMessage)))
      //          case Success(offsets) =>
      //            val sorted = ListMap(offsets.toSeq.sortBy(_._1): _*)
      //            Ok(views.html.topicOffset(formValidationResult, Some(sorted)))
      //        }
      //      })
    })
  }


  //  private def getMessages(topic: String, ts: Long, count: Int): Future[Try[Iterable[MessagesResponse]]] = {
  //    //next offset for each partition after give ts
  //    val beginOffsetsFuture = searchOffset(topic, ts).map(_.partitionOffset)
  //    val messages = beginOffsetsFuture.flatMap { tryGetBeginOffsets =>
  //      val responses = tryGetBeginOffsets match {
  //        case Failure(e) => Future.successful(Failure(e))
  //        case Success(beginOffsets) => Future.sequence(beginOffsets.map {
  //            case (partition, offsetBehindTs) => {
  //              val messageRequest = MessagesRequest(-1, TopicPartition(topic, partition), offsetBehindTs.offset, count)
  //              (messageLookupSupervisorActor ? messageRequest).mapTo[MessagesResponse]
  //            }
  //          }).map(Success(_))
  //      }
  //      responses
  //    }
  //    messages
  //  }

  def messagesWithCountGet = Action { implicit request =>
    Ok(views.html.messagesWithCount(GetMessagesWithMaxCountForm.form, None))
  }

  def getMessages(topic: String, maxCount: Int, partitionOffsets: Map[Int, OffsetBehindTs]): Future[Map[Int, Either[Seq[StringMessage], String]]] = {
    val messageResponseFuture = Future.sequence(partitionOffsets.map {
      case (partitionId, offsetBehindTs) => (messageLookupSupervisorActor ? MessagesRequest(-1, TopicPartition(topic, partitionId), offsetBehindTs.offset, maxCount)).mapTo[MessagesResponse].map(response =>
        (response.request.tp.partition, response.result match {
          case Success(messages) => Left(messages.map(StringMessage(_)))
          case Failure(e) => Right(e.getMessage)
        }))
    }).map(_.toMap)
    messageResponseFuture
  }

  def messagesWithCountPost = Action.async { implicit request =>
    val formValidationResult = GetMessagesWithMaxCountForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.messagesWithCount(formWithErrors, None)))
    }, { form =>
      val GetMessagesWithMaxCountForm(topic, dateTime, maxCount) = form
      val partitionOffsetsFuture = (offsetLookupSupervisorActor ? FindTopicOffsetRequest(-1, topic, dateTime.getMillis)).mapTo[FindTopicOffsetResponse].map(_.partitionOffset)
      partitionOffsetsFuture.flatMap {
        case Failure(e) => Future.successful(Ok(views.html.messagesWithCount(formValidationResult, None, Some(e.getMessage))))
        case Success(partitionOffsets) =>
          getMessages(topic, maxCount, partitionOffsets)
            .map(partitionedMessages => {
              Ok(views.html.messagesWithCount(formValidationResult, Some(partitionedMessages), None))
            })
      }
    }
    )
  }

  def produceGet = Action { implicit request =>
    Ok(views.html.produce(ProduceForm.form, None))
  }

  def producePost = Action.async { implicit request =>
    val formValidationResult = ProduceForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.produce(formWithErrors, None)))
    }, { form =>
      val record = new ProducerRecord[String, String](form.topic, form.partition, form.key, form.value)
      (produceSupervisorActor ? record).mapTo[Try[RecordMetadata]].map {
        case Success(metadata) => Ok(views.html.produce(formValidationResult, Some(Left(metadata))))
        case Failure(e) => Ok(views.html.produce(formValidationResult, Some(Right(e.getMessage))))
      }
    })
  }

  def describeTopicGet = Action { implicit request =>
    Ok(views.html.topicDescription(TopicForm.form, None))
  }

  def describeTopicPost = Action.async { implicit request =>
    val formValidationResult = TopicForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.topicDescription(formWithErrors, None)))
    }, { form =>
      val topic = form.topic
      describeTopic(topic)
    }
    )
  }

  def describeTopic(topic: String)(implicit request: Request[AnyContent]): Future[Result] = {
    val response = (metadataSupervisorActor ? DescribeTopicRequest(-1, topic)).mapTo[DescribeTopicResponse].map(_.result)
    val form = TopicForm.form.fill(new TopicForm(topic))
    response map {
      case Failure(e) => Ok(views.html.topicDescription(form, None, Some(e.getMessage)))
      case Success(topicMeta) => Ok(views.html.topicDescription(form, Some(PartitionedTopicMetadata(topicMeta)), None))
    }
  }

  def createTopicsGet = Action { implicit request =>
    Ok(views.html.createTopics(CreateTopicsForm.form, 1, None))
  }

  def createTopicsPost = Action { implicit request =>
    val body = request.body.asFormUrlEncoded
    println(body)
    val formValidationResult = CreateTopicsForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => BadRequest(views.html.createTopics(formWithErrors, formValidationResult.data.size, None))
    }, { form =>
      newTopics(form.topics)
    }
    )
  }

  def addTopicsGet() = Action { implicit request =>
    Ok(views.html.batchTopicAddition())
  }

  private def newTopics(topics: Seq[NewTopic]): Result = {
    topics.foreach { topic =>
      topicCommandSupervisorActor ! topic
    }
    Ok("done")
  }


  def addTopicsPost() = Action { implicit request => {
    val topicsOpt = request.body.asFormUrlEncoded.flatMap(_.get("topics").map(_.head))
    Try {
      val topics = topicsOpt.map { topics =>
        val splits = topics.split("\\r\\n|\\n|\\r").map(_.split(","))
        splits.map(info => NewTopic(info(0).trim, info(1).trim.toInt, info(2).trim.toInt)).toList
      }
      topics.map(newTopics).getOrElse(Ok("no topics to add"))
    } match {
      case Success(result) => result
      case Failure(e) =>
        logger.error("", e)
        Ok(e.toString)
    }
  }
  }


  def createTopicGet = Action.async { implicit request =>
    val brokersFutrue = (topicCommandSupervisorActor ? GetBrokerMetadatas).mapTo[Try[Seq[BrokerMetadata]]]
    brokersFutrue.map {
      //      case Success(brokers) => Ok(views.html.createSingleTopic(List(197,198,199)))
      case Success(brokers) => Ok(views.html.createSingleTopic(brokers.map(_.id)))
      case Failure(e) => Ok(views.html.createSingleTopic(Seq.empty[Int], Some(e.toString)))
    }
  }

  def createTopicPost = Action.async { implicit request =>
    val userInput = request.body.asFormUrlEncoded.get
    val topic = userInput.get("topicName").get.head
    val partitions = userInput.get("partitions").get.head.toInt
    val replicationFactor = userInput.get("replicationFactor").get.head.toInt
    val replicas = userInput.keySet.filter(_.startsWith("broker-")).map(_.split('-')(1).toInt)
    val response = (topicCommandSupervisorActor ? NewTopicWithAssignment(topic, partitions, replicationFactor, replicas)).mapTo[Try[Unit]]
    response.flatMap {
      case _: Success[Unit] => describeTopic(topic)
      case Failure(e) => Future.successful(Ok(views.html.createSingleTopic(List.empty[Int], Some(e.toString))))
    }
  }

  def rebalanceGet = Action { implicit request =>
    Ok(views.html.rebalance(ConsumerGroupForm.form, None))
  }

  def rebalancePost = Action.async { implicit request =>
    val formValidationResult = ConsumerGroupForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.rebalance(formWithErrors, None)))
    }, { form =>
      (adminSupervisorActor ? TriggerRebalance(form.group)).mapTo[Try[Unit]] flatMap {
        case Success(_) => {
          describeConsumerGroup(form.group)
        }
        case Failure(e) => Future.successful(Ok(views.html.rebalance(formValidationResult, Some(e.toString))))
      }
    }
    )
  }

  def resetGroupOffsetGet = Action { implicit request =>
    Ok(views.html.resetGroupOffsets(GroupAndDateTimeForm.form, None))
  }

  def resetGroupOffsetPost = Action.async { implicit request =>
    val formValidationResult = GroupAndDateTimeForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.resetGroupOffsets(formWithErrors, None)))
    }, { form =>
      val ts = form.time.getMillis
      (adminSupervisorActor.ask(ResetGroupOffsets(form.group, ts))(Timeout(60 seconds))).mapTo[Try[Boolean]].flatMap {
        case Success(true) =>
          describeConsumerGroup(form.group)
        case Success(false) =>
          describeConsumerGroup(form.group, Some("Failed to reset group progress"))
        case Failure(e) =>
          Future.successful(Ok(views.html.resetGroupOffsets(formValidationResult, Some(e.toString))))
      }
    }
    )
  }

  def getMessageByTopicPartitionOffsetGet = Action { implicit request =>
    Ok(views.html.topicPartitionOffset(TopicPartitionOffsetForm.form))
  }

  def getMessageByTopicPartitionOffsetPost = Action.async { implicit request =>
    val formValidationResult = TopicPartitionOffsetForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.topicPartitionOffset(formWithErrors, None)))
    }, { form =>
      val TopicPartitionOffsetForm(topic, partition, offset) = form
      val messageRequest = MessageRequest(id = -1, tp = new TopicPartition(topic, partition), offset)
      (messageLookupSupervisorActor ? messageRequest).mapTo[MessageResponse].map(_.result).map {
        case Success(r) =>
          val strMessage = StringMessage(r)
          Ok(views.html.topicPartitionOffset(formValidationResult, Some(Left(strMessage))))
        case Failure(e) => Ok(views.html.topicPartitionOffset(formValidationResult, None, Some(e.toString)))
      }
    })
  }


  def resetGroupTopicOffsetGet = Action { implicit request =>
    Ok(views.html.resetGroupTopicOffsets(GroupTopicDateTimeForm.form, None))
  }

  def resetGroupTopicOffsetPost = Action.async { implicit request =>
    val formValidationResult = GroupTopicDateTimeForm.form.bindFromRequest()
    formValidationResult.fold({
      formWithErrors => Future.successful(BadRequest(views.html.resetGroupTopicOffsets(formWithErrors, None)))
    }, { form =>
      val GroupTopicDateTimeForm(group, topic, dateTime) = form
      (metadataSupervisorActor ? TopicExistRequest(-1, topic)).mapTo[TopicExistResponse].map(_.existed).flatMap{
        case false => Future.successful(Ok(views.html.resetGroupTopicOffsets(formValidationResult, Some("topic not exist"))))
        case true => {
          (adminSupervisorActor.ask(ResetGroupTopicOffset(form.group, topic, dateTime.getMillis))(Timeout(60 seconds))).mapTo[Try[Boolean]].flatMap {
            case Success(true) =>
              describeConsumerGroup(form.group)
            case Success(false) =>
              describeConsumerGroup(form.group, Some("Failed to reset group progress"))
            case Failure(e) =>
              Future.successful(Ok(views.html.resetGroupTopicOffsets(formValidationResult, Some(e.toString))))
          }
        }
      }
    }
    )
  }
}