"""Pay-per-event contract: one charge per organization actually monitored.

The unit is a *completed* monitoring pass over one valid organization number.
Nothing else is billable, and the run allocates its worst-case charge before it
spends the source's time, so a watchlist that cannot be paid for is never
partially checked.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from norway_brreg_company_change_monitor.models import EVENT_COMPANY_MONITORED

#: Published price. 200 companies watched daily is about $3/month, 1,000 about
#: $15/month and 5,000 about $75/month - deliberately under the Norwegian SaaS
#: monitoring envelope while keeping one transparent per-watchlist-item unit.
COMPANY_MONITORED_PRICE_USD = Decimal("0.0005")

#: Events the platform may add on its own. They are tolerated at any price so a
#: publisher-side synthetic default cannot make the Actor refuse to run, but they
#: are still deducted from the budget before work is allocated.
SYNTHETIC_EVENTS = frozenset({"actor-start", "apify-actor-start", "actor-start-gb"})


class PricingContractError(ValueError):
    """The configured pricing is not the contract this Actor implements."""


def affordable_targets(manager: Any) -> int | None:
    """How many organizations this run may charge for. ``None`` means unmetered.

    Raises rather than guessing if any publisher-defined event other than
    ``company-monitored`` carries a price: automatic Dataset billing would charge
    diagnostics and recovery rows, which this Actor promises never to do.
    """
    pricing = manager.get_pricing_info()
    if not pricing.is_pay_per_event:
        return None
    prices = pricing.per_event_prices
    company_price = Decimal(str(prices.get(EVENT_COMPANY_MONITORED, 0)))
    if not company_price.is_finite() or company_price <= 0:
        raise PricingContractError(
            "Pay-per-event pricing must define a positive company-monitored price."
        )
    for event, value in prices.items():
        price = Decimal(str(value))
        if not price.is_finite() or price < 0:
            raise PricingContractError(f"Event {event} has an unusable price.")
        if price > 0 and event != EVENT_COMPANY_MONITORED and event not in SYNTHETIC_EVENTS:
            raise PricingContractError(
                f"Unexpected paid event {event}; only company-monitored may be charged by this "
                "Actor. Disable the extra event before publishing."
            )
    remaining = (
        Decimal(str(pricing.max_total_charge_usd)) - manager.calculate_total_charged_amount()
    )
    if not remaining.is_finite():
        return None
    return max(0, int(remaining // company_price))


def billing_summary(
    *, charged: int, price: Decimal = COMPANY_MONITORED_PRICE_USD
) -> dict[str, Any]:
    return {
        "charged_companies": charged,
        "charged_event": EVENT_COMPANY_MONITORED,
        "charged_amount_usd": float(price * charged),
    }
