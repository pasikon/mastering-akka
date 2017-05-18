package com.packt.masteringakka.bookstore.order

import akka.actor.{ActorIdentity, ActorRef, FSM, Identify}
import com.packt.masteringakka.bookstore.common.{Empty, FullResult, ServiceResult}
import com.packt.masteringakka.bookstore.domain.book.{Book, FindBook}
import com.packt.masteringakka.bookstore.domain.user.{BookstoreUser, FindUserById}

import concurrent.duration._

/**
  * Created by michalz on 17/05/2017.
  */
class SalesOrderProcessor extends FSM[SalesOrderProcessor.State, SalesOrderProcessor.Data] {

  import SalesOrderProcessor._

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
        val newData = identifier match {
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
        user ! FindUserById(inputs.request.userId)
        val expectedBooks = inputs.request.lineItems.map(_.bookId).toSet
        expectedBooks.foreach(id => book ! FindBook(id))
        goto(LookingUpEntities) using (ResolvedDependencies(inputs, expectedBooks, None, Map.empty, book, credit))
    }

    when(LookingUpEntities, 5 seconds) {
      transform{
        case Event(FullResult(b: Book), stateData: ResolvedDependencies) =>
          val lineItemForBook = stateData.inputs.request.lineItems.find(_.bookId == b.id)
          lineItemForBook match {
            case None =>
              stateData.originator ! unexpectedFail
              stop()
          }

        case Event(bUsermaybe: ServiceResult[BookstoreUser], stateData: ResolvedDependencies) =>
          val newData = bUsermaybe match {
            case full: FullResult[BookstoreUser] =>
              stateData.copy(user = Some(full.value))
            case empty: Empty => stateData //TODO: short circuit
          }
          stay using newData
        case Event(bookMaybe: ServiceResult[Book], stateData: ResolvedDependencies) =>
          val newData = bookMaybe match {
            case full: FullResult[Book] =>
              stateData.copy(books = stateData.books + (full.value.id -> full.value))
            case empty: Empty => stateData //TODO: short circuit
          }
          stay using newData
      } using {

      }
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

}
