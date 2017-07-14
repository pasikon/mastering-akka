package com.packt.masteringakka.bookstore.order

import java.util.Date

import akka.actor.FSM.Failure
import akka.actor.{ActorIdentity, ActorRef, FSM, Identify}
import com.packt.masteringakka.bookstore.common._
import com.packt.masteringakka.bookstore.domain.book.{Book, FindBook}
import com.packt.masteringakka.bookstore.domain.credit.{ChargeCreditCard, CreditCardTransaction, CreditTransactionStatus}
import com.packt.masteringakka.bookstore.domain.user.{BookstoreUser, FindUserById}
import com.packt.masteringakka.bookstore.order.SalesOrderProcessor.{InvalidBookIdError, InvalidUserIdError}

import concurrent.duration._
import scala.language.postfixOps

/**
  * Created by michalz on 17/05/2017.
  */
class SalesOrderProcessor extends FSM[SalesOrderProcessor.State, SalesOrderProcessor.Data] {

  import SalesOrderProcessor._

  def unexpectedFail = Failure(FailureType.Service, ServiceResult.UnexpectedFailure )

  //  val dao = new SalesOrderPr

  startWith(Idle, Uninitialized)

  when(Idle) { //wraps every incoming msg with Event[D](event: Any, stateData: D)
    case Event(req: CreateOrder, _) =>
      lookup(SalesOrderManager.BookMgrName) ! Identify(ResolutionIdent.Book)
      lookup(SalesOrderManager.UserManagerName) ! Identify(ResolutionIdent.User)
      lookup(SalesOrderManager.CreditHandlerName) ! Identify(ResolutionIdent.Credit)
      goto(ResolvingDependencies) using UnresolvedDependencies(Inputs(sender(), req), None, None)
  }

  when(ResolvingDependencies, 5 seconds) {
    transform { // transform is gatekeeper for going to next transition
      case Event(ActorIdentity(identifier: ResolutionIdent.Value, actor@Some(ref)), stateData: UnresolvedDependencies) =>
        log.info("Resolved dependency {}, {}", identifier, ref)
        val newData: UnresolvedDependencies = identifier match {
          case ResolutionIdent.Book =>
            stateData.copy(bookMgr = actor)
          case ResolutionIdent.User =>
            stateData.copy(userMgr = actor)
          case ResolutionIdent.Book =>
            stateData.copy(creditHandler = actor)
        }
        stay using newData
    } using {
      //here evaluating current state if ready to go to another
      case FSM.State(state, UnresolvedDependencies(inputs, Some(user), Some(book), Some(credit)), _, _, _) =>

        log.info("Resolved all dependencies, looking up entities")

        user ! FindUserById(inputs.request.userId)
        val expectedBooks = inputs.request.lineItems.map(_.bookId).toSet
        expectedBooks.foreach(id => book ! FindBook(id))
        goto(LookingUpEntities) using ResolvedDependencies(inputs, expectedBooks, None, Map.empty, book, credit)
    }
  }

  when(LookingUpEntities, 5 seconds) {
    transform{
      case Event(FullResult(b: Book), stateData: ResolvedDependencies) =>
        val lineItemForBook = stateData.inputs.request.lineItems.find(_.bookId == b.id)
        lineItemForBook match {
          case None =>
            stateData.originator ! unexpectedFail
            stop()

          case Some(item) if item.quantity > b.inventoryAmount =>
            stateData.originator ! Failure(FailureType.Validation, ErrorMessage("order.inventory.notavailable", Some("Inventory for an item on this order is no longer available")))
            stop

          case _ => stay using stateData.copy(books = stateData.books ++ Map(b.id -> b))
        }

      case Event(FullResult(u: BookstoreUser), stateData: ResolvedDependencies) =>
        stay using stateData.copy(user = Some(u))

      case Event(EmptyResult, data:ResolvedDependencies) =>
        val (etype, error) =
          if (sender().path.name == BookMgrName) ("book", InvalidBookIdError)
          else ("user", InvalidUserIdError )
        log.info("Unexpected result type of EmptyResult received looking up a {} entity", etype)
        data.originator ! Failure(FailureType.Validation, error)
        stop

    } using {
      case FSM.State(state, ResolvedDependencies(inputs, expectedBooks, Some(u), bookMap, userMgr, creditMgr), _, _, _)
        if bookMap.keySet == expectedBooks =>

        log.info("Successfully looked up all entities and inventory is available, charging credit card")
        val lineItems = inputs.request.lineItems.
          flatMap{item =>
            bookMap.
              get(item.bookId).
              map(b => SalesOrderLineItem(0, 0, b.id, item.quantity, item.quantity * b.cost, new Date(), new Date()))
          }

        val total = lineItems.map(_.cost).sum
        creditMgr ! ChargeCreditCard(inputs.request.cardInfo, total)
        goto(ChargingCard) using LookedUpData(inputs, u, lineItems, total)
    }
  }

  when(ChargingCard, 5 seconds) {
    transform{
      case Event(FullResult(cct: CreditCardTransaction), stateData: LookedUpData) if cct.status == CreditTransactionStatus.Approved =>
        stay using stateData
      case _ =>
        log.info("Failed to pay with credit card!")
        stateData.originator ! Failure(FailureType.Validation, "Credit card payment failed")
        stop
    } using{
      case FSM.State(state, LookedUpData(inputs, user, items, total), _, _, _) =>
        goto(ChargingCard)
    }
  }

  def lookup(name: String) = context.actorSelection(s"/user/$name")
}

object ResolutionIdent extends Enumeration {
  val Book, User, Credit = Value
}

object SalesOrderProcessor {

  sealed trait State

  sealed trait Data {
    def originator: ActorRef
  }

  trait InputsData extends Data {
    def inputs: Inputs

    def originator: ActorRef = inputs.originator
  }

  case class Inputs(originator: ActorRef, request: CreateOrder)

  case class UnresolvedDependencies(inputs: Inputs,
                                    userMgr: Option[ActorRef],
                                    bookMgr: Option[ActorRef],
                                    creditHandler: Option[ActorRef] = None) extends InputsData

  case class ResolvedDependencies(inputs: Inputs,
                                  expectedBooks: Set[Int],
                                  user: Option[BookstoreUser],
                                  books: Map[Int, Book],
                                  bookMgr: ActorRef,
                                  creditHandler: ActorRef) extends InputsData

  case class LookedUpData(inputs: Inputs,
                          user: BookstoreUser,
                          items: List[SalesOrderLineItem],
                          total: Double) extends InputsData {

  }

  case object Idle extends State

  case object ResolvingDependencies extends State

  case object LookingUpEntities extends State

  case object ChargingCard extends State

  case object WritingEntity extends State

  case object Uninitialized extends Data {
    override def originator: ActorRef = ActorRef.noSender
  }

  val UserManagerName = "user-manager"
  val CreditHandlerName = "credit-handler"
  val BookMgrName = "book-manager"

  val InvalidBookIdError = ErrorMessage("order.invalid.bookId", Some("You have supplied an invalid book id"))
  val InvalidUserIdError = ErrorMessage("order.invalid.userId", Some("You have supplied an invalid user id"))
  val CreditRejectedError = ErrorMessage("order.credit.rejected", Some("Your credit card has been rejected"))
  val InventoryNotAvailError = ErrorMessage("order.inventory.notavailable", Some("Inventory for an item on this order is no longer available"))

}
