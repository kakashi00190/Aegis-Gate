import random

ADJECTIVES = [
    "Brave", "Silent", "Wild", "Dark", "Swift", "Iron", "Storm", "Frost", "Shadow", "Gold",
    "Ancient", "Fierce", "Noble", "Savage", "Crimson", "Azure", "Phantom", "Steel", "Jade", "Rogue",
    "Mystic", "Lunar", "Solar", "Thunder", "Ember", "Venom", "Titan", "Omega", "Alpha", "Void",
    "Sharp", "Cosmic", "Neon", "Chaos", "Eternal", "Wicked", "Holy", "Cursed", "Mighty", "Hollow",
    "Silver", "Rugged", "Grim", "Crystal", "Frozen", "Blazing", "Electric", "Toxic", "Spectral", "Divine",
    "Ruthless", "Cunning", "Rapid", "Deadly", "Covert", "Stealthy", "Furious", "Glorious", "Infinite", "Broken",
]

ANIMALS = [
    "Wolf", "Eagle", "Tiger", "Falcon", "Viper", "Cobra", "Panther", "Bear", "Fox", "Hawk",
    "Dragon", "Lion", "Shark", "Raven", "Lynx", "Jaguar", "Scorpion", "Rhino", "Condor", "Manta",
    "Puma", "Raptor", "Kraken", "Phoenix", "Hydra", "Basilisk", "Griffin", "Chimera", "Wyvern", "Mantis",
    "Badger", "Wolverine", "Orca", "Stallion", "Crow", "Osprey", "Moose", "Coyote", "Pelican", "Gecko",
    "Mamba", "Dingo", "Hyena", "Narwhal", "Piranha", "Tarantula", "Barracuda", "Mastiff", "Jackal", "Bison",
]


def generate_anonymous_name() -> str:
    adj = random.choice(ADJECTIVES)
    animal = random.choice(ANIMALS)
    number = random.randint(10, 99)
    return f"{adj}{animal}{number}"
