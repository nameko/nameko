"""
In a large integration test of highly connected services, it's preferable to
limit interactions to only those under test.

This file defines several toy services that interact to form a shop of the
famous ACME Corporation. The AcmeShopService relies on the StockService,
InvoiceService and PaymentService to fulfil its orders. They are not best
practice examples! They're minimal services provided for the test at the
bottom of the file.

``test_shop_integration`` is a  full integration test of the ACME shop
"checkout" flow. It demonstrates how to test the multiple ACME services in
combination with each other, including limiting service interactions by
replacing certain entrypoint and injection dependencies.
"""

from collections import defaultdict

import pytest

from nameko.dependencies import (
    InjectionProvider, injection, DependencyFactory)
from nameko.events import event_dispatcher, Event, event_handler
from nameko.exceptions import RemoteError
from nameko.rpc import rpc, rpc_proxy
from nameko.runners import ServiceRunner
from nameko.standalone.rpc import RpcProxy
from nameko.testing.services import replace_injections, restrict_entrypoints
from nameko.testing.utils import get_container
from nameko.timer import timer


class NotLoggedIn(Exception):
    pass


class ItemOutOfStock(Exception):
    pass


class ItemDoesNotExist(Exception):
    pass


class ShoppingBasket(InjectionProvider):
    """ A shopping basket tied to the current ``user_id``.
    """
    def __init__(self):
        self.baskets = defaultdict(list)

    def acquire_injection(self, worker_ctx):

        class Basket(object):
            def __init__(self, basket):
                self._basket = basket
                self.worker_ctx = worker_ctx

            def add(self, item):
                self._basket.append(item)

            def __iter__(self):
                for item in self._basket:
                    yield item

        try:
            user_id = worker_ctx.data['user_id']
        except KeyError:
            raise NotLoggedIn()
        return Basket(self.baskets[user_id])


@injection
def shopping_basket():
    """ A shopping basket tied to the current user.
    """
    return DependencyFactory(ShoppingBasket)


class ItemAddedToBasket(Event):
    """ Dispatched when an item is added to a shopping basket.
    """
    type = "item_added_to_basket"


class CheckoutComplete(Event):
    """ Dispatched when the checkout process completes
    """
    type = "checkout_complete"


class AcmeShopService(object):

    user_basket = shopping_basket()
    stock_service = rpc_proxy('stockservice')
    invoice_service = rpc_proxy('invoiceservice')
    payment_service = rpc_proxy('paymentservice')

    fire_event = event_dispatcher()

    @rpc
    def add_to_basket(self, item_code):
        """ Add item identified by ``item_code`` to the shopping basket.

        This is a toy example! Ignore the obvious race condition.
        """
        stock_level = self.stock_service.check_stock(item_code)
        if stock_level > 0:
            self.user_basket.add(item_code)
            self.fire_event(ItemAddedToBasket(item_code))
            return item_code

        raise ItemOutOfStock(item_code)

    @rpc
    def checkout(self):
        """ Take payment for all items in the shopping basket.
        """
        total_price = sum(self.stock_service.check_price(item)
                          for item in self.user_basket)

        # prepare invoice
        invoice = self.invoice_service.prepare_invoice(total_price)

        # take payment
        self.payment_service.take_payment(invoice)

        # fire checkout event if prepare_invoice and take_payment succeeded
        checkout_event = CheckoutComplete({
            'invoice': invoice,
            'items': list(self.user_basket)
        })
        self.fire_event(checkout_event)
        return total_price


class Warehouse(InjectionProvider):
    """ A database of items in the warehouse.

    This is a toy example! A dictionary is not a database.
    """
    def __init__(self):
        self.database = {
            'anvil': {
                'price': 100,
                'stock': 3
            },
            'dehydrated_boulders': {
                'price': 999,
                'stock': 12
            },
            'invisible_paint': {
                'price': 10,
                'stock': 30
            },
            'toothpicks': {
                'price': 1,
                'stock': 0
            }
        }

    def acquire_injection(self, worker_ctx):
        return self.database


@injection
def warehouse():
    """ A shopping basket tied to the current user.
    """
    return DependencyFactory(Warehouse)


class StockService(object):

    warehouse = warehouse()

    @rpc
    def check_price(self, item_code):
        """ Check the price of an item.
        """
        try:
            return self.warehouse[item_code]['price']
        except KeyError:
            raise ItemDoesNotExist(item_code)

    @rpc
    def check_stock(self, item_code):
        """ Check the stock level of an item.
        """
        try:
            return self.warehouse[item_code]['stock']
        except KeyError:
            raise ItemDoesNotExist(item_code)

    @rpc
    @timer(100)
    def monitor_stock(self):
        """ Periodic stock monitoring method. Can also be triggered manually
        over RPC.

        This is an expensive process that we don't want to exercise during
        integration testing...
        """
        raise NotImplemented()

    @event_handler('acmeshopservice', CheckoutComplete.type)
    def dispatch_items(self, event_data):
        """ Dispatch items from stock on successful checkouts.

        This is an expensive process that we don't want to exercise during
        integration testing...
        """
        raise NotImplemented()


class AddressBook(InjectionProvider):
    """ A database of user details, keyed on user_id.
    """
    def __init__(self):
        self.address_book = {
            'wile_e_coyote': {
                'username': 'wile_e_coyote',
                'fullname': 'Wile E Coyote',
                'address': '12 Long Road, High Cliffs, Utah',
            },
        }

    def acquire_injection(self, worker_ctx):
        def get_user_details():
            try:
                user_id = worker_ctx.data['user_id']
            except KeyError:
                raise NotLoggedIn()
            return self.address_book.get(user_id)
        return get_user_details


@injection
def address_book():
    """ Provides the address of the current user.
    """
    return DependencyFactory(AddressBook)


class InvoiceService(object):

    get_user_details = address_book()

    @rpc
    def prepare_invoice(self, amount):
        """ Prepare an invoice for ``amount`` for the current user.
        """
        address = self.get_user_details().get('address')
        fullname = self.get_user_details().get('fullname')
        username = self.get_user_details().get('username')

        msg = "Dear {}. Please pay ${} to ACME Corp.".format(fullname, amount)
        invoice = {
            'message': msg,
            'amount': amount,
            'customer': username,
            'address': address
        }
        return invoice


class PaymentService(object):

    @rpc
    def take_payment(self, invoice):
        """ Take payment from a customer according to ``invoice``.

        This is an expensive process that we don't want to exercise during
        integration testing...
        """
        raise NotImplemented()

# =============================================================================
# Begin test
# =============================================================================


@pytest.yield_fixture
def runner_factory(rabbit_config):

    all_runners = []

    def make_runner(*service_classes):
        runner = ServiceRunner(rabbit_config)
        for service_cls in service_classes:
            runner.add_service(service_cls)
        all_runners.append(runner)
        return runner

    yield make_runner

    for r in all_runners:
        try:
            r.stop()
        except:
            pass


@pytest.yield_fixture
def rpc_proxy_factory(rabbit_config):
    """ Factory fixture for standalone RPC proxies.

    Proxies are started automatically so they can be used without a ``with``
    statement. All created proxies are stopped at the end of the test, when
    this fixture closes.
    """
    all_proxies = []

    def make_proxy(service_name, **kwargs):
        proxy = RpcProxy(service_name, rabbit_config, **kwargs)
        all_proxies.append(proxy)
        return proxy.start()

    yield make_proxy

    for proxy in all_proxies:
        proxy.stop()


def test_shop_checkout_integration(runner_factory, rpc_proxy_factory):
    """ Simulate a checkout flow as an integration test.

    Requires instances of AcmeShopService, StockService and InvoiceService
    to be running. Explicitly replaces the rpc proxy to PaymentService so
    that service doesn't need to be hosted.

    Also replaces the event dispatcher injection on AcmeShopService and
    disables the timer entrypoint on StockService. Limiting the interactions
    of services in this way reduces the scope of the integration test and
    eliminates undesirable side-effects (e.g. processing events unnecessarily).
    """
    context_data = {'user_id': 'wile_e_coyote'}
    shop = rpc_proxy_factory('acmeshopservice', context_data=context_data)

    runner = runner_factory(AcmeShopService, StockService, InvoiceService)

    # replace ``event_dispatcher`` and ``payment_service``  injections on
    # AcmeShopService with Mock injections
    shop_container = get_container(runner, AcmeShopService)
    fire_event, payment_service = replace_injections(
        shop_container, "fire_event", "payment_service")

    # restrict entrypoints on StockService
    stock_container = get_container(runner, StockService)
    restrict_entrypoints(stock_container, "check_price", "check_stock")

    runner.start()

    # add some items to the basket
    assert shop.add_to_basket("anvil") == "anvil"
    assert shop.add_to_basket("invisible_paint") == "invisible_paint"

    # try to buy something that's out of stock
    with pytest.raises(RemoteError) as exc_info:
        shop.add_to_basket("toothpicks")
    assert exc_info.value.exc_type == "ItemOutOfStock"

    # provide a mock response from the payment service
    payment_service.take_payment.return_value = "Payment complete."

    # checkout
    res = shop.checkout()

    total_amount = 100 + 10
    assert res == total_amount

    # verify integration with mocked out payment service
    payment_service.take_payment.assert_called_once_with({
        'customer': "wile_e_coyote",
        'address': "12 Long Road, High Cliffs, Utah",
        'amount': total_amount,
        'message': "Dear Wile E Coyote. Please pay $110 to ACME Corp."
    })

    # verify events fired as expected
    assert fire_event.call_count == 3


if __name__ == "__main__":
    import sys
    pytest.main(sys.argv)
