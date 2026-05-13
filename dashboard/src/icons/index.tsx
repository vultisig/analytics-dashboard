/**
 * Vultisig icon set — name-mapped to Tabler Icons.
 *
 * Each export matches a Figma component name (Icon* in the
 * Vultisig-App design library) so renamed/added icons land naturally
 * in the right consumer. The default size is 16 with stroke 1.5 to
 * match the Figma renders.
 */
import {
    IconLayoutGrid,
    IconArrowsLeftRight,
    IconWallet,
    IconUsers,
    IconHash,
    IconUserPlus,
    IconTrophy,
    IconCalendar,
    IconChartLine,
    IconCurrencyDollar,
    IconShieldLock,
    IconArrowUpRight,
    IconInfoCircle,
    IconCalculator,
    IconChartBar,
    IconReceipt2,
    IconArrowsExchange,
    IconLayoutSidebarLeftCollapse,
    IconLayoutSidebarLeftExpand,
    IconSearch,
    IconChevronDown,
    IconChevronRight,
    IconChevronLeft,
    IconPlus,
    IconMinus,
    IconCheck,
    IconX,
    IconExternalLink,
    IconTrendingUp,
    IconTrendingDown,
    IconCoin,
    IconDiamond,
    IconEye,
    IconEyeOff,
    IconAlertCircle,
    IconLoader2,
    IconShield,
    IconAward,
    IconPigMoney,
    IconActivity,
    type IconProps,
} from '@tabler/icons-react';
import { forwardRef } from 'react';
import type { ForwardRefExoticComponent, RefAttributes } from 'react';

type TablerIcon = ForwardRefExoticComponent<IconProps & RefAttributes<SVGSVGElement>>;

/** Wraps a Tabler icon with the Vultisig-friendly defaults (size 16,
 *  stroke 1.5) so consumers can drop them in without per-call props. */
function vultisig(name: string, Base: TablerIcon): TablerIcon {
    const Wrapped = forwardRef<SVGSVGElement, IconProps>(function VultisigIcon(props, ref) {
        return (
            <Base
                ref={ref}
                size={props.size ?? 16}
                stroke={props.stroke ?? 1.5}
                {...props}
            />
        );
    });
    Wrapped.displayName = name;
    return Wrapped;
}

/* === Figma → Tabler mapping === */
export const IconSquareGridCircle = vultisig('IconSquareGridCircle', IconLayoutGrid);
export const IconArrowLeftRight   = vultisig('IconArrowLeftRight',   IconArrowsLeftRight);
export const IconWallet4          = vultisig('IconWallet4',          IconWallet);
export const IconPeopleCopy       = vultisig('IconPeopleCopy',       IconUsers);
export const IconHashtag          = vultisig('IconHashtag',          IconHash);
export const IconPeopleAdded      = vultisig('IconPeopleAdded',      IconUserPlus);
export const IconTrophyV          = vultisig('IconTrophy',           IconTrophy);
export const IconCalendar3        = vultisig('IconCalendar3',        IconCalendar);
export const IconLineChart1       = vultisig('IconLineChart1',       IconChartLine);
export const IconDollar           = vultisig('IconDollar',           IconCurrencyDollar);
export const IconVault            = vultisig('IconVault',            IconShieldLock);
export const IconArrowUpRightV    = vultisig('IconArrowUpRight',     IconArrowUpRight);
export const IconCircleInfo       = vultisig('IconCircleInfo',       IconInfoCircle);
export const IconCalculatorV      = vultisig('IconCalculator',       IconCalculator);
export const IconChart4           = vultisig('IconChart4',           IconChartBar);
export const IconReceiptCheck     = vultisig('IconReceiptCheck',     IconReceipt2);
export const IconArrowsRepeatLR   = vultisig('IconArrowsRepeatLeftRight', IconArrowsExchange);
export const IconLayoutLeft       = vultisig('IconLayoutLeft',       IconLayoutSidebarLeftCollapse);
export const IconLayoutLeftExpand = vultisig('IconLayoutLeftExpand', IconLayoutSidebarLeftExpand);
export const IconMagnifyingGlass2 = vultisig('IconMagnifyingGlass2', IconSearch);
export const IconChevronDownSmall = vultisig('IconChevronDownSmall', IconChevronDown);
export const IconChevronRightV    = vultisig('IconChevronRight',     IconChevronRight);
export const IconChevronLeftV     = vultisig('IconChevronLeft',      IconChevronLeft);

/* additional utility icons (not Figma-named) */
export const IconPlusV       = vultisig('IconPlus',       IconPlus);
export const IconMinusV      = vultisig('IconMinus',      IconMinus);
export const IconCheckV      = vultisig('IconCheck',      IconCheck);
export const IconXV          = vultisig('IconX',          IconX);
export const IconExternalLinkV = vultisig('IconExternalLink', IconExternalLink);
export const IconTrendingUpV   = vultisig('IconTrendingUp',   IconTrendingUp);
export const IconTrendingDownV = vultisig('IconTrendingDown', IconTrendingDown);
export const IconCoinV         = vultisig('IconCoin',         IconCoin);
export const IconGemV          = vultisig('IconGem',          IconDiamond);
export const IconEyeV          = vultisig('IconEye',          IconEye);
export const IconEyeOffV       = vultisig('IconEyeOff',       IconEyeOff);
export const IconAlertCircleV  = vultisig('IconAlertCircle',  IconAlertCircle);
export const IconLoader2V      = vultisig('IconLoader2',      IconLoader2);
export const IconShieldV       = vultisig('IconShield',       IconShield);
export const IconAwardV        = vultisig('IconAward',        IconAward);
export const IconPigMoneyV     = vultisig('IconPigMoney',     IconPigMoney);
export const IconActivityV     = vultisig('IconActivity',     IconActivity);
