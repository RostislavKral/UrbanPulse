import type { VehicleMode } from "../types/vehicle";

const ICON_SIZE = 64;

type IconDefinition = {
  body: string;
};

type IconMapping = Record<
  VehicleMode,
  {
    x: number;
    y: number;
    width: number;
    height: number;
    mask: boolean;
  }
>;

const iconDefinitions: Record<VehicleMode, IconDefinition> = {
  bus: {
    body: `
      <rect x="14" y="19" width="36" height="28" rx="6"/>
      <rect x="18" y="24" width="9" height="8" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <rect x="31" y="24" width="15" height="8" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <rect x="19" y="37" width="26" height="4" rx="2" fill="white"/>
      <circle cx="22" cy="49" r="4"/>
      <circle cx="42" cy="49" r="4"/>
    `,
  },
  tram: {
    body: `
      <path d="M25 10 L32 4 L39 10" fill="none" stroke="black" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>
      <line x1="32" y1="10" x2="32" y2="17" stroke="black" stroke-width="5" stroke-linecap="round"/>
      <rect x="17" y="17" width="30" height="33" rx="7"/>
      <rect x="21" y="23" width="10" height="9" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <rect x="35" y="23" width="8" height="9" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <path d="M23 40 H41" fill="none" stroke="white" stroke-width="4" stroke-linecap="round"/>
      <circle cx="24" cy="53" r="3"/>
      <circle cx="40" cy="53" r="3"/>
    `,
  },
  metro: {
    body: `
      <path d="M20 19 C20 12 44 12 44 19 V43 C44 48 40 52 35 52 H29 C24 52 20 48 20 43 Z"/>
      <rect x="24" y="23" width="16" height="11" rx="3" fill="none" stroke="black" stroke-width="4"/>
      <path d="M25 41 H39" fill="none" stroke="white" stroke-width="4" stroke-linecap="round"/>
      <path d="M27 54 H37" fill="none" stroke="black" stroke-width="5" stroke-linecap="round"/>
      <circle cx="26" cy="44" r="2.5" fill="white"/>
      <circle cx="38" cy="44" r="2.5" fill="white"/>
    `,
  },
  rail: {
    body: `
      <path d="M18 18 C18 11 46 11 46 18 V42 C46 49 41 54 34 54 H30 C23 54 18 49 18 42 Z"/>
      <rect x="22" y="23" width="8" height="10" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <rect x="34" y="23" width="8" height="10" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <path d="M22 43 H42" fill="none" stroke="white" stroke-width="4" stroke-linecap="round"/>
      <path d="M25 58 L31 50 M39 58 L33 50" fill="none" stroke="black" stroke-width="4" stroke-linecap="round"/>
    `,
  },
  ferry: {
    body: `
      <path d="M14 38 H50 L44 50 H20 Z"/>
      <rect x="23" y="24" width="18" height="12" rx="3"/>
      <rect x="28" y="18" width="8" height="6" rx="2"/>
      <path d="M18 54 C23 50 27 58 32 54 C37 50 41 58 46 54" fill="none" stroke="black" stroke-width="4" stroke-linecap="round"/>
      <rect x="27" y="28" width="10" height="4" rx="2" fill="white"/>
    `,
  },
  trolleybus: {
    body: `
      <path d="M24 17 L18 7 M40 17 L46 7" fill="none" stroke="black" stroke-width="4" stroke-linecap="round"/>
      <path d="M16 7 H48" fill="none" stroke="black" stroke-width="4" stroke-linecap="round"/>
      <rect x="14" y="19" width="36" height="28" rx="6"/>
      <rect x="18" y="24" width="10" height="8" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <rect x="32" y="24" width="14" height="8" rx="2" fill="none" stroke="black" stroke-width="4"/>
      <path d="M20 39 H44" fill="none" stroke="white" stroke-width="4" stroke-linecap="round"/>
      <circle cx="22" cy="49" r="4"/>
      <circle cx="42" cy="49" r="4"/>
    `,
  },
  unknown: {
    body: `
      <path d="M32 9 L51 21 V43 L32 55 L13 43 V21 Z"/>
      <circle cx="32" cy="44" r="3.5" fill="white"/>
      <path d="M25 26 C25 21 29 18 33 18 C39 18 42 21 42 26 C42 30 39 32 35 34 C32 36 32 37 32 39" fill="none" stroke="white" stroke-width="5" stroke-linecap="round"/>
    `,
  },
};

const iconModes: VehicleMode[] = [
  "tram",
  "metro",
  "rail",
  "bus",
  "ferry",
  "trolleybus",
  "unknown",
];

const iconPictogram = (mode: VehicleMode, index: number): string => {
  const x = index * ICON_SIZE;

  return `
    <g transform="translate(${x} 0)" fill="black" stroke="none">
      ${iconDefinitions[mode].body}
    </g>
  `;
};

const atlasSvg = `
  <svg xmlns="http://www.w3.org/2000/svg" width="${ICON_SIZE * iconModes.length}"
    height="${ICON_SIZE}" viewBox="0 0 ${ICON_SIZE * iconModes.length} ${ICON_SIZE}">
    ${iconModes.map(iconPictogram).join("")}
  </svg>
`;

export const VEHICLE_ICON_ATLAS = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(
  atlasSvg,
)}`;

export const VEHICLE_ICON_MAPPING: IconMapping = Object.fromEntries(
  iconModes.map((mode, index) => [
    mode,
    {
      x: index * ICON_SIZE,
      y: 0,
      width: ICON_SIZE,
      height: ICON_SIZE,
      mask: false,
    },
  ]),
) as IconMapping;

export function vehicleIconName(mode: VehicleMode | undefined): VehicleMode {
  return mode && mode in VEHICLE_ICON_MAPPING ? mode : "unknown";
}

export function vehicleModeColor(
  mode: VehicleMode | undefined,
): [number, number, number] {
  switch (mode) {
    case "tram":
      return [79, 195, 247];
    case "metro":
      return [171, 120, 255];
    case "rail":
      return [255, 213, 79];
    case "bus":
      return [102, 224, 164];
    case "ferry":
      return [77, 208, 225];
    case "trolleybus":
      return [255, 167, 38];
    default:
      return [207, 216, 220];
  }
}
